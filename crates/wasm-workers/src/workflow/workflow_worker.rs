use super::event_history::ApplyError;
use super::workflow_ctx::{WorkflowCtx, WorkflowFunctionError};
use crate::WasmFileError;
use crate::workflow::workflow_ctx::{ImportedFnCall, WorkerPartialResult};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use concepts::storage::{DbConnection, DbPool};
use concepts::time::{ClockFn, Sleep, now_tokio_instant};
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
use utils::wasm_tools::{ExIm, WasmComponent};
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
impl Default for JoinNextBlockingStrategy {
    fn default() -> Self {
        JoinNextBlockingStrategy::Await {
            non_blocking_event_batching: DEFAULT_NON_BLOCKING_EVENT_BATCHING,
        }
    }
}

#[derive(Clone, Debug)]
pub struct WorkflowConfig {
    pub component_id: ComponentId,
    pub join_next_blocking_strategy: JoinNextBlockingStrategy,
    pub retry_on_trap: bool,
    pub forward_unhandled_child_errors_in_join_set_close: bool,
    pub backtrace_persist: bool,
}

pub struct WorkflowWorkerCompiled<C: ClockFn, S: Sleep> {
    config: WorkflowConfig,
    engine: Arc<Engine>,
    clock_fn: C,
    sleep: S,
    wasm_component: WasmComponent,
}

pub struct WorkflowWorkerLinked<C: ClockFn, S: Sleep, DB: DbConnection, P: DbPool<DB>> {
    config: WorkflowConfig,
    engine: Arc<Engine>,
    exim: ExIm,
    clock_fn: C,
    sleep: S,
    exported_ffqn_to_index: hashbrown::HashMap<FunctionFqn, ComponentExportIndex>,
    fn_registry: Arc<dyn FunctionRegistry>,
    instance_pre: InstancePre<WorkflowCtx<C, DB, P>>,
}

#[derive(Clone)]
pub struct WorkflowWorker<C: ClockFn, S: Sleep, DB: DbConnection, P: DbPool<DB>> {
    config: WorkflowConfig,
    engine: Arc<Engine>,
    exim: ExIm,
    db_pool: P,
    clock_fn: C,
    sleep: S,
    exported_ffqn_to_index: hashbrown::HashMap<FunctionFqn, ComponentExportIndex>,
    fn_registry: Arc<dyn FunctionRegistry>,
    instance_pre: InstancePre<WorkflowCtx<C, DB, P>>,
}

impl<C: ClockFn, S: Sleep> WorkflowWorkerCompiled<C, S> {
    #[tracing::instrument(skip_all, fields(%config.component_id))]
    pub fn new_with_config(
        wasm_component: WasmComponent,
        config: WorkflowConfig,
        engine: Arc<Engine>,
        clock_fn: C,
        sleep: S,
    ) -> Self {
        Self {
            config,
            engine,
            clock_fn,
            sleep,
            wasm_component,
        }
    }

    #[instrument(skip_all, fields(component_id = %self.config.component_id))]
    pub fn link<DB: DbConnection, P: DbPool<DB>>(
        self,
        fn_registry: Arc<dyn FunctionRegistry>,
    ) -> Result<WorkflowWorkerLinked<C, S, DB, P>, WasmFileError> {
        let mut linker = wasmtime::component::Linker::new(&self.engine);

        // Link obelisk:workflow-support and obelisk:log
        WorkflowCtx::add_to_linker(&mut linker)?;

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
                        move |mut store_ctx: wasmtime::StoreContextMut<
                            '_,
                            WorkflowCtx<C, DB, P>,
                        >,
                              params: &[Val],
                              results: &mut [Val]| {
                            let imported_fn_call = match ImportedFnCall::new(
                                ffqn.clone(),
                                &mut store_ctx,
                                params,
                                self.config.backtrace_persist,
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
                                Ok(store_ctx
                                    .data_mut()
                                    .call_imported_fn(imported_fn_call, results, ffqn)
                                    .await?)
                            })
                        }
                    });
                    if let Err(err) = res {
                        if err.to_string() == format!("import `{function_name}` not found") {
                            // FIXME: Add test for error message stability
                            warn!("Skipping mocking of {ffqn}");
                        } else {
                            return Err(WasmFileError::LinkingError {
                                context: StrVariant::Arc(Arc::from(format!(
                                    "cannot add mock for imported function {ffqn}"
                                ))),
                                err: err.into(),
                            });
                        }
                    }
                }
            } else {
                warn!("Skipping interface {ifc_fqn}", ifc_fqn = import.ifc_fqn);
            }
        }

        // Pre-instantiate to catch missing imports
        let instance_pre = linker
            .instantiate_pre(&self.wasm_component.wasmtime_component)
            .map_err(|err| WasmFileError::LinkingError {
                context: StrVariant::Static("preinstantiation error"),
                err: err.into(),
            })?;

        let exported_ffqn_to_index = self
            .wasm_component
            .index_exported_functions()
            .map_err(WasmFileError::DecodeError)?;

        Ok(WorkflowWorkerLinked {
            config: self.config,
            engine: self.engine,
            exim: self.wasm_component.exim,
            clock_fn: self.clock_fn,
            sleep: self.sleep,
            exported_ffqn_to_index,
            fn_registry,
            instance_pre,
        })
    }

    pub fn exported_functions_ext(&self) -> &[FunctionMetadata] {
        self.wasm_component.exim.get_exports(true)
    }

    pub fn exports_hierarchy_ext(&self) -> &[PackageIfcFns] {
        self.wasm_component.exim.get_exports_hierarchy_ext()
    }

    pub fn imported_functions(&self) -> &[FunctionMetadata] {
        &self.wasm_component.exim.imports_flat
    }
}

impl<C: ClockFn, S: Sleep, DB: DbConnection, P: DbPool<DB>> WorkflowWorkerLinked<C, S, DB, P> {
    pub fn into_worker(self, db_pool: P) -> WorkflowWorker<C, S, DB, P> {
        WorkflowWorker {
            config: self.config,
            engine: self.engine,
            exim: self.exim,
            db_pool,
            clock_fn: self.clock_fn,
            sleep: self.sleep,
            exported_ffqn_to_index: self.exported_ffqn_to_index,
            fn_registry: self.fn_registry,
            instance_pre: self.instance_pre,
        }
    }
}

enum RunError<C: ClockFn + 'static, DB: DbConnection + 'static, P: DbPool<DB> + 'static> {
    ResultParsingError(ResultParsingError, WorkflowCtx<C, DB, P>),
    /// Error from the wasmtime runtime that can be downcast to `WorkflowFunctionError`
    WorkerPartialResult(WorkerPartialResult, WorkflowCtx<C, DB, P>),
    /// Error that happened while running the function, which cannot be downcast to `WorkflowFunctionError`
    Trap {
        reason: String,
        detail: String,
        workflow_ctx: WorkflowCtx<C, DB, P>,
        kind: TrapKind,
    },
}

enum WorkerResultRefactored<C: ClockFn, DB: DbConnection, P: DbPool<DB>> {
    Ok(SupportedFunctionReturnValue, WorkflowCtx<C, DB, P>),
    FatalError(FatalError, WorkflowCtx<C, DB, P>),
    Retriable(WorkerResult),
}

type CallFuncResult<C, DB, P> =
    Result<(SupportedFunctionReturnValue, WorkflowCtx<C, DB, P>), RunError<C, DB, P>>;

impl<C: ClockFn + 'static, S: Sleep + 'static, DB: DbConnection + 'static, P: DbPool<DB> + 'static>
    WorkflowWorker<C, S, DB, P>
{
    async fn prepare_func(
        &self,
        ctx: WorkerContext,
    ) -> Result<
        (
            Store<WorkflowCtx<C, DB, P>>,
            wasmtime::component::Func,
            Arc<[Val]>,
        ),
        WorkerError,
    > {
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
            self.fn_registry.clone(),
            ctx.worker_span,
            self.config.forward_unhandled_child_errors_in_join_set_close,
            self.config.backtrace_persist,
        );

        let mut store = Store::new(&self.engine, workflow_ctx);

        // Configure epoch callback before running the initialization to avoid interruption
        store.epoch_deadline_callback(|_store_ctx| Ok(UpdateDeadline::Yield(1)));

        let instance = match self.instance_pre.instantiate_async(&mut store).await {
            Ok(instance) => instance,
            Err(err) => {
                let reason = err.to_string();
                let version = store.into_data().version;
                if reason.starts_with("maximum concurrent") {
                    return Err(WorkerError::LimitReached { reason, version });
                }
                return Err(WorkerError::FatalError(
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

        let params = match ctx.params.as_vals(func.params(&store)) {
            Ok(params) => params,
            Err(err) => {
                return Err(WorkerError::FatalError(
                    FatalError::ParamsParsingError(err),
                    version_at_start,
                ));
            }
        };
        Ok((store, func, params))
    }

    async fn call_func(
        mut store: Store<WorkflowCtx<C, DB, P>>,
        func: wasmtime::component::Func,
        params: Arc<[Val]>,
    ) -> CallFuncResult<C, DB, P> {
        let result_types = func.results(&mut store);
        let mut results = vec![Val::Bool(false); result_types.len()];
        let func_call_result = func.call_async(&mut store, &params, &mut results).await;
        if func_call_result.is_ok() {
            if let Err(post_return_err) = func.post_return_async(&mut store).await {
                return Err(RunError::Trap {
                    reason: post_return_err.to_string(),
                    detail: format!("{post_return_err:?}"),
                    workflow_ctx: store.into_data(),
                    kind: TrapKind::PostReturnTrap,
                });
            }
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
                } else {
                    Err(RunError::Trap {
                        reason: err.to_string(),
                        detail: format!("{err:?}"),
                        workflow_ctx,
                        kind: TrapKind::Trap,
                    })
                }
            }
        }
    }

    async fn race_func_with_timeout(
        &self,
        store: Store<WorkflowCtx<C, DB, P>>,
        func: wasmtime::component::Func,
        params: Arc<[Val]>,
        worker_span: Span,
        execution_deadline: DateTime<Utc>,
    ) -> WorkerResult {
        let started_at = self.clock_fn.now();
        let Ok(deadline_duration) = (execution_deadline - started_at).to_std() else {
            worker_span.in_scope(||
                info!(%execution_deadline, %started_at, "Timed out - started_at later than execution_deadline")
            );
            return WorkerResult::DbUpdatedByWorkerOrWatcher;
        };
        let stopwatch = now_tokio_instant(); // Not using `clock_fn` here is ok, value is only used for log reporting.
        tokio::select! { // future's liveness: Dropping the loser immediately.
            res = Self::race_func_internal(store, func, params, &worker_span, stopwatch, deadline_duration, execution_deadline, self.config.retry_on_trap) => {
                res
            },
            () = self.sleep.sleep(deadline_duration) => {
                // Not flushing the workflow_ctx as it would introduce locking.
                // Not closing the join sets, workflow MUST be retried.
                worker_span.in_scope(||
                    info!(duration = ?stopwatch.elapsed(), ?deadline_duration, %execution_deadline, now = %self.clock_fn.now(), "Timed out")
                );

                WorkerResult::DbUpdatedByWorkerOrWatcher
            }
        }
    }

    #[expect(clippy::too_many_arguments)]
    async fn race_func_internal(
        store: Store<WorkflowCtx<C, DB, P>>,
        func: wasmtime::component::Func,
        params: Arc<[Val]>,
        worker_span: &Span,
        stopwatch: tokio::time::Instant,
        deadline_duration: Duration,
        execution_deadline: DateTime<Utc>,
        retry_on_trap: bool,
    ) -> WorkerResult {
        let res = Self::call_func(store, func, params).await;
        let worker_result_refactored = Self::convert_result(
            res,
            worker_span,
            stopwatch,
            deadline_duration,
            execution_deadline,
            retry_on_trap,
        )
        .await;
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

    async fn convert_result(
        res: CallFuncResult<C, DB, P>,
        worker_span: &Span,
        stopwatch: tokio::time::Instant,
        deadline_duration: Duration,
        execution_deadline: DateTime<Utc>,
        retry_on_trap: bool,
    ) -> WorkerResultRefactored<C, DB, P> {
        match res {
            Ok((supported_result, mut workflow_ctx)) => {
                worker_span.in_scope(||
                    info!(duration = ?stopwatch.elapsed(), ?deadline_duration, %execution_deadline, "Finished")
                );
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
                        error!("Database flush error: {db_err:?} while handling: {detail}, execution will be retried")
                    );
                    return WorkerResultRefactored::Retriable(WorkerResult::Err(
                        WorkerError::DbError(db_err),
                    ));
                }
                let version = workflow_ctx.version;
                let err = if retry_on_trap {
                    worker_span.in_scope(||
                        info!(duration = ?stopwatch.elapsed(), ?deadline_duration, %execution_deadline, "Trap handled as an temporary error")
                    );
                    WorkerError::TemporaryWorkflowTrap {
                        reason,
                        kind,
                        detail: Some(detail),
                        version,
                    }
                } else {
                    worker_span.in_scope(||
                        info!(duration = ?stopwatch.elapsed(), ?deadline_duration, %execution_deadline, "Trap handled as a fatal error")
                    );
                    WorkerError::FatalError(
                        FatalError::WorkflowTrap {
                            reason,
                            trap_kind: kind,
                            detail,
                        },
                        version,
                    )
                };

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
                        worker_span.in_scope(|| info!(duration = ?stopwatch.elapsed(), ?deadline_duration, %execution_deadline, "Finished with a fatal error: {err}"));
                        WorkerResultRefactored::FatalError(err, workflow_ctx)
                    }
                    WorkerPartialResult::InterruptRequested => {
                        worker_span.in_scope(|| info!(duration = ?stopwatch.elapsed(), ?deadline_duration, %execution_deadline, "Interrupt requested"));
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

    async fn close_join_sets(workflow_ctx: &mut WorkflowCtx<C, DB, P>) -> Result<(), WorkerResult> {
        workflow_ctx
            .close_opened_join_sets()
            .await
            .map_err(|err| match err {
                ApplyError::NondeterminismDetected(detail) => {
                    WorkerResult::Err(WorkerError::FatalError(
                        FatalError::NondeterminismDetected { detail },
                        workflow_ctx.version.clone(),
                    ))
                }
                ApplyError::InterruptRequested => WorkerResult::DbUpdatedByWorkerOrWatcher,
                ApplyError::DbError(db_error) => WorkerResult::Err(WorkerError::DbError(db_error)),
                // Can only happen when forwarding unhandled child errors in join set close.
                ApplyError::UnhandledChildExecutionError {
                    child_execution_id,
                    root_cause_id,
                } => WorkerResult::Err(WorkerError::FatalError(
                    FatalError::UnhandledChildExecutionError {
                        child_execution_id,
                        root_cause_id,
                    },
                    workflow_ctx.version.clone(),
                )),
            })
    }
}

#[async_trait]
impl<C: ClockFn + 'static, S: Sleep + 'static, DB: DbConnection + 'static, P: DbPool<DB> + 'static>
    Worker for WorkflowWorker<C, S, DB, P>
{
    fn exported_functions(&self) -> &[FunctionMetadata] {
        self.exim.get_exports(false)
    }

    fn imported_functions(&self) -> &[FunctionMetadata] {
        &self.exim.imports_flat
    }

    async fn run(&self, ctx: WorkerContext) -> WorkerResult {
        trace!("Run with ctx: {ctx:?}");
        if !ctx.can_be_retried {
            warn!(
                "Workflow configuration set to not retry anymore. This can lead to nondeterministic results."
            );
        }
        let worker_span = ctx.worker_span.clone();
        worker_span.in_scope(|| info!("Execution run started"));
        let execution_deadline = ctx.execution_deadline;
        let (store, func, params) = match self.prepare_func(ctx).await {
            Ok(ok) => ok,
            Err(err) => return WorkerResult::Err(err), // errors here cannot affect joinset closing, as they happen before fn invocation
        };
        self.race_func_with_timeout(store, func, params, worker_span, execution_deadline)
            .await
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::{
        activity::activity_worker::tests::{
            FIBO_10_INPUT, FIBO_10_OUTPUT, compile_activity, spawn_activity_fibo, wasm_file_name,
        },
        engines::{EngineConfig, Engines},
        tests::{TestingFnRegistry, fn_registry_dummy},
    };
    use assert_matches::assert_matches;
    use concepts::time::TokioSleep;
    use concepts::{
        ComponentType,
        prefixed_ulid::{ExecutorId, RunId},
        storage::{
            CreateRequest, DbConnection, PendingState, PendingStateFinished,
            PendingStateFinishedResultKind, Version, wait_for_pending_state_fn,
        },
    };
    use concepts::{ExecutionId, Params};
    use db_tests::Database;
    use executor::executor::extract_ffqns_test;
    use executor::{
        executor::{ExecConfig, ExecTask, ExecutorTaskHandle},
        expired_timers_watcher,
    };
    use rstest::rstest;
    use serde_json::json;
    use std::time::Duration;
    use test_utils::sim_clock::SimClock;
    use tracing::info_span;
    use val_json::{
        type_wrapper::TypeWrapper,
        wast_val::{WastVal, WastValWithType},
    };

    pub const FIBO_WORKFLOW_FFQN: FunctionFqn = FunctionFqn::new_static_tuple(
        test_programs_fibo_workflow_builder::exports::testing::fibo_workflow::workflow::FIBOA,
    ); // fiboa: func(n: u8, iterations: u32) -> u64;
    const SLEEP_HOST_ACTIVITY_FFQN: FunctionFqn =
        FunctionFqn::new_static_tuple(test_programs_sleep_workflow_builder::exports::testing::sleep_workflow::workflow::SLEEP_HOST_ACTIVITY); // sleep-host-activity: func(millis: u64);

    const TICK_SLEEP: Duration = Duration::from_millis(1);

    #[cfg(not(madsim))]
    const FFQN_WORKFLOW_HTTP_GET_STARGAZERS: FunctionFqn = FunctionFqn::new_static_tuple(
        test_programs_http_get_workflow_builder::exports::testing::http_workflow::workflow::GET_STARGAZERS);

    #[cfg(not(madsim))]
    const FFQN_WORKFLOW_HTTP_GET: FunctionFqn = FunctionFqn::new_static_tuple(
        test_programs_http_get_workflow_builder::exports::testing::http_workflow::workflow::GET,
    );
    #[cfg(not(madsim))]
    const FFQN_WORKFLOW_HTTP_GET_SUCCESSFUL: FunctionFqn = FunctionFqn::new_static_tuple(test_programs_http_get_workflow_builder::exports::testing::http_workflow::workflow::GET_SUCCESSFUL);
    #[cfg(not(madsim))]
    const FFQN_WORKFLOW_HTTP_GET_RESP: FunctionFqn = FunctionFqn::new_static_tuple(test_programs_http_get_workflow_builder::exports::testing::http_workflow::workflow::GET_RESP);

    pub(crate) async fn compile_workflow(wasm_path: &str) -> (WasmComponent, ComponentId) {
        let engine = Engines::get_workflow_engine(EngineConfig::on_demand_testing().await).unwrap();
        compile_workflow_with_engine(wasm_path, &engine)
    }

    pub(crate) fn compile_workflow_with_engine(
        wasm_path: &str,
        engine: &Engine,
    ) -> (WasmComponent, ComponentId) {
        let component_id =
            ComponentId::new(ComponentType::Workflow, wasm_file_name(wasm_path)).unwrap();
        (
            WasmComponent::new(wasm_path, engine, Some(component_id.component_type.into()))
                .unwrap(),
            component_id,
        )
    }

    async fn spawn_workflow<DB: DbConnection + 'static, P: DbPool<DB> + 'static>(
        db_pool: P,
        wasm_path: &'static str,
        clock_fn: impl ClockFn + 'static,
        join_next_blocking_strategy: JoinNextBlockingStrategy,
        fn_registry: Arc<dyn FunctionRegistry>,
    ) -> ExecutorTaskHandle {
        let workflow_engine =
            Engines::get_workflow_engine(EngineConfig::on_demand_testing().await).unwrap();
        let component_id =
            ComponentId::new(ComponentType::Workflow, wasm_file_name(wasm_path)).unwrap();
        let worker = Arc::new(
            WorkflowWorkerCompiled::new_with_config(
                WasmComponent::new(
                    wasm_path,
                    &workflow_engine,
                    Some(component_id.component_type.into()),
                )
                .unwrap(),
                WorkflowConfig {
                    component_id: component_id.clone(),
                    join_next_blocking_strategy,
                    retry_on_trap: false,
                    forward_unhandled_child_errors_in_join_set_close: false,
                    backtrace_persist: false,
                },
                workflow_engine,
                clock_fn.clone(),
                TokioSleep,
            )
            .link(fn_registry)
            .unwrap()
            .into_worker(db_pool.clone()),
        );
        info!("Instantiated worker");
        let exec_config = ExecConfig {
            batch_size: 1,
            lock_expiry: Duration::from_secs(3),
            tick_sleep: TICK_SLEEP,
            component_id,
            task_limiter: None,
        };
        ExecTask::spawn_new(
            worker,
            exec_config,
            clock_fn,
            db_pool,
            ExecutorId::generate(),
        )
    }

    pub(crate) async fn spawn_workflow_fibo<DB: DbConnection + 'static, P: DbPool<DB> + 'static>(
        db_pool: P,
        clock_fn: impl ClockFn + 'static,
        join_next_blocking_strategy: JoinNextBlockingStrategy,
        fn_registry: Arc<dyn FunctionRegistry>,
    ) -> ExecutorTaskHandle {
        spawn_workflow(
            db_pool,
            test_programs_fibo_workflow_builder::TEST_PROGRAMS_FIBO_WORKFLOW,
            clock_fn,
            join_next_blocking_strategy,
            fn_registry,
        )
        .await
    }

    #[rstest]
    #[tokio::test]
    async fn fibo_workflow_should_schedule_fibo_activity_mem(
        #[values(JoinNextBlockingStrategy::Interrupt, JoinNextBlockingStrategy::Await { non_blocking_event_batching: 0}, JoinNextBlockingStrategy::Await { non_blocking_event_batching: 10})]
        join_next_blocking_strategy: JoinNextBlockingStrategy,
    ) {
        let sim_clock = SimClock::default();
        let (_guard, db_pool) = Database::Memory.set_up().await;
        fibo_workflow_should_schedule_fibo_activity(
            db_pool.clone(),
            sim_clock,
            join_next_blocking_strategy,
        )
        .await;
        db_pool.close().await.unwrap();
    }

    #[cfg(not(madsim))]
    #[rstest]
    #[tokio::test]
    async fn fibo_workflow_should_schedule_fibo_activity_sqlite(
        #[values(JoinNextBlockingStrategy::Interrupt, JoinNextBlockingStrategy::Await { non_blocking_event_batching: 0}, JoinNextBlockingStrategy::Await { non_blocking_event_batching: 10})]
        join_next_blocking_strategy: JoinNextBlockingStrategy,
    ) {
        let sim_clock = SimClock::default();
        let (_guard, db_pool) = Database::Sqlite.set_up().await;
        fibo_workflow_should_schedule_fibo_activity(
            db_pool.clone(),
            sim_clock,
            join_next_blocking_strategy,
        )
        .await;
        db_pool.close().await.unwrap();
    }

    async fn fibo_workflow_should_schedule_fibo_activity<
        DB: DbConnection + 'static,
        P: DbPool<DB> + 'static,
    >(
        db_pool: P,
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
        let workflow_exec_task = spawn_workflow_fibo(
            db_pool.clone(),
            sim_clock.clone(),
            join_next_blocking_strategy,
            fn_registry,
        )
        .await;
        // Create an execution.
        let execution_id = ExecutionId::generate();
        let created_at = sim_clock.now();
        let db_connection = db_pool.connection();

        let params = Params::from_json_values(vec![json!(FIBO_10_INPUT), json!(INPUT_ITERATIONS)]);
        db_connection
            .create(CreateRequest {
                created_at,
                execution_id: execution_id.clone(),
                ffqn: FIBO_WORKFLOW_FFQN,
                params,
                parent: None,
                metadata: concepts::ExecutionMetadata::empty(),
                scheduled_at: created_at,
                retry_exp_backoff: Duration::ZERO,
                max_retries: u32::MAX,
                component_id: ComponentId::dummy_workflow(),
                scheduled_by: None,
            })
            .await
            .unwrap();
        info!("Should end as BlockedByJoinSet");
        wait_for_pending_state_fn(
            &db_connection,
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
        let activity_exec_task =
            spawn_activity_fibo(db_pool.clone(), sim_clock.clone(), TokioSleep).await;

        let res = db_connection
            .wait_for_finished_result(&execution_id, None)
            .await
            .unwrap()
            .unwrap();
        let res =
            assert_matches!(res, SupportedFunctionReturnValue::InfallibleOrResultOk(val) => val);
        let res = assert_matches!(res, WastValWithType{ value: WastVal::U64(actual), r#type: TypeWrapper::U64} => actual);
        assert_eq!(FIBO_10_OUTPUT, res,);
        workflow_exec_task.close().await;
        activity_exec_task.close().await;
    }

    #[tokio::test]
    #[should_panic(expected = "LinkingError { context: preinstantiation error")]
    async fn fibo_workflow_with_missing_imports_should_fail() {
        let sim_clock = SimClock::default();
        let (_guard, db_pool) = Database::Memory.set_up().await;
        test_utils::set_up();
        let fn_registry = fn_registry_dummy(&[]);
        spawn_workflow_fibo(
            db_pool.clone(),
            sim_clock.clone(),
            JoinNextBlockingStrategy::Interrupt,
            fn_registry,
        )
        .await;
        db_pool.close().await.unwrap();
    }

    pub(crate) async fn compile_workflow_worker<
        C: ClockFn,
        S: Sleep,
        DB: DbConnection + 'static,
        P: DbPool<DB> + 'static,
    >(
        wasm_path: &str,
        db_pool: P,
        clock_fn: C,
        sleep: S,
        join_next_blocking_strategy: JoinNextBlockingStrategy,
        fn_registry: Arc<dyn FunctionRegistry>,
    ) -> Arc<WorkflowWorker<C, S, DB, P>> {
        let workflow_engine =
            Engines::get_workflow_engine(EngineConfig::on_demand_testing().await).unwrap();
        Arc::new(
            WorkflowWorkerCompiled::new_with_config(
                WasmComponent::new(
                    wasm_path,
                    &workflow_engine,
                    Some(ComponentType::Workflow.into()),
                )
                .unwrap(),
                WorkflowConfig {
                    component_id: ComponentId::dummy_workflow(),
                    join_next_blocking_strategy,
                    retry_on_trap: false,
                    forward_unhandled_child_errors_in_join_set_close: false,
                    backtrace_persist: false,
                },
                workflow_engine,
                clock_fn,
                sleep,
            )
            .link(fn_registry)
            .unwrap()
            .into_worker(db_pool),
        )
    }

    #[tokio::test]
    async fn execution_deadline_before_now_should_timeout() {
        const SLEEP_MILLIS: u32 = 100;
        test_utils::set_up();

        let (_guard, db_pool) = Database::Memory.set_up().await;

        let sim_clock = SimClock::epoch();
        let worker = compile_workflow_worker(
            test_programs_sleep_workflow_builder::TEST_PROGRAMS_SLEEP_WORKFLOW,
            db_pool.clone(),
            sim_clock.clone(),
            TokioSleep,
            JoinNextBlockingStrategy::Interrupt,
            TestingFnRegistry::new_from_components(vec![
                compile_activity(
                    test_programs_sleep_activity_builder::TEST_PROGRAMS_SLEEP_ACTIVITY,
                )
                .await,
                compile_workflow(
                    test_programs_sleep_workflow_builder::TEST_PROGRAMS_SLEEP_WORKFLOW,
                )
                .await,
            ]),
        )
        .await;
        // simulate a scheduling problem where deadline < now
        let execution_deadline = sim_clock.now();
        sim_clock
            .move_time_forward(Duration::from_millis(100))
            .await;

        let ctx = WorkerContext {
            execution_id: ExecutionId::generate(),
            metadata: concepts::ExecutionMetadata::empty(),
            ffqn: SLEEP_HOST_ACTIVITY_FFQN,
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
        assert_matches!(worker_result, WorkerResult::DbUpdatedByWorkerOrWatcher);
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
        let (_guard, db_pool) = database.set_up().await;

        let fn_registry = TestingFnRegistry::new_from_components(vec![
            compile_activity(test_programs_sleep_activity_builder::TEST_PROGRAMS_SLEEP_ACTIVITY) // not used here
                .await,
            compile_workflow(test_programs_sleep_workflow_builder::TEST_PROGRAMS_SLEEP_WORKFLOW)
                .await,
        ]);

        let execution_id = ExecutionId::generate();
        let db_connection = db_pool.connection();
        db_connection
            .create(CreateRequest {
                created_at: sim_clock.now(),
                execution_id: execution_id.clone(),
                ffqn: SLEEP_HOST_ACTIVITY_FFQN,
                params: Params::from_json_values(vec![json!({"milliseconds": SLEEP_MILLIS})]),
                parent: None,
                metadata: concepts::ExecutionMetadata::empty(),
                scheduled_at: sim_clock.now(),
                retry_exp_backoff: Duration::ZERO,
                max_retries: u32::MAX,
                component_id: ComponentId::dummy_workflow(),
                scheduled_by: None,
            })
            .await
            .unwrap();

        let sleep_exec = {
            let worker = compile_workflow_worker(
                test_programs_sleep_workflow_builder::TEST_PROGRAMS_SLEEP_WORKFLOW,
                db_pool.clone(),
                sim_clock.clone(),
                TokioSleep,
                join_next_blocking_strategy,
                fn_registry,
            )
            .await;
            let exec_config = ExecConfig {
                batch_size: 1,
                lock_expiry: LOCK_DURATION,
                tick_sleep: Duration::ZERO, // irrelevant here as we call tick manually
                component_id: ComponentId::dummy_workflow(),
                task_limiter: None,
            };
            let ffqns = extract_ffqns_test(worker.as_ref());
            ExecTask::new(
                worker,
                exec_config,
                sim_clock.clone(),
                db_pool.clone(),
                ffqns,
            )
        };
        {
            let worker_tasks = sleep_exec
                .tick_test(sim_clock.now())
                .await
                .unwrap()
                .wait_for_tasks()
                .await
                .unwrap();
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
        sim_clock
            .move_time_forward(Duration::from_millis(u64::from(SLEEP_MILLIS)))
            .await;

        // Run the worker tick again before the response arrives - should be noop
        {
            let worker_tasks = sleep_exec
                .tick_test(sim_clock.now())
                .await
                .unwrap()
                .wait_for_tasks()
                .await
                .unwrap();
            assert_eq!(0, worker_tasks);
        }
        // expired_timers_watcher should see the AsyncDelay and send the response.
        {
            let timer = expired_timers_watcher::tick_test(&db_connection, sim_clock.now())
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
                    sim_clock.move_time_to(blocked_until).await;
                }
            }
        }
        // Reexecute the worker
        {
            let worker_tasks = sleep_exec
                .tick_test(sim_clock.now())
                .await
                .unwrap()
                .wait_for_tasks()
                .await
                .unwrap();
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

        drop(db_connection);
        db_pool.close().await.unwrap();
    }

    #[cfg(not(madsim))]
    #[rstest]
    #[tokio::test]
    async fn stargazers_should_be_deserialized_after_interrupt(
        #[values(Database::Sqlite, Database::Memory)] db: Database,
    ) {
        use crate::activity::activity_worker::tests::spawn_activity;

        test_utils::set_up();
        let sim_clock = SimClock::new(DateTime::default());
        let (_guard, db_pool) = db.set_up().await;
        let created_at = sim_clock.now();
        let db_connection = db_pool.connection();
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
        let activity_exec_task = spawn_activity(
            db_pool.clone(),
            test_programs_http_get_activity_builder::TEST_PROGRAMS_HTTP_GET_ACTIVITY,
            sim_clock.clone(),
            TokioSleep,
        )
        .await;

        let workflow_exec_task = spawn_workflow(
            db_pool.clone(),
            test_programs_http_get_workflow_builder::TEST_PROGRAMS_HTTP_GET_WORKFLOW,
            sim_clock.clone(),
            JoinNextBlockingStrategy::Interrupt,
            fn_registry,
        )
        .await;
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
                retry_exp_backoff: Duration::ZERO,
                max_retries: u32::MAX,
                component_id: ComponentId::dummy_workflow(),
                scheduled_by: None,
            })
            .await
            .unwrap();
        // Check the result.
        let res = assert_matches!(
            db_connection
                .wait_for_finished_result(&execution_id, None)
                .await
                .unwrap(),
            Ok(res) => res
        );
        assert_eq!(SupportedFunctionReturnValue::None, res);
        drop(db_connection);
        activity_exec_task.close().await;
        workflow_exec_task.close().await;
        db_pool.close().await.unwrap();
    }

    #[cfg(not(madsim))]
    #[rstest]
    #[tokio::test]
    async fn http_get(
        #[values(JoinNextBlockingStrategy::Interrupt, JoinNextBlockingStrategy::Await { non_blocking_event_batching: 0}, JoinNextBlockingStrategy::Await { non_blocking_event_batching: 10})]
        join_next_blocking_strategy: JoinNextBlockingStrategy,
        #[values(
            FFQN_WORKFLOW_HTTP_GET,
            FFQN_WORKFLOW_HTTP_GET_RESP,
            FFQN_WORKFLOW_HTTP_GET_SUCCESSFUL
        )]
        ffqn: FunctionFqn,
        #[values(Database::Sqlite, Database::Memory)] db: Database,
    ) {
        use crate::activity::activity_worker::tests::spawn_activity;
        use chrono::DateTime;
        use std::ops::Deref;
        use tracing::debug;
        use wiremock::{
            Mock, MockServer, ResponseTemplate,
            matchers::{method, path},
        };
        const BODY: &str = "ok";

        test_utils::set_up();
        let sim_clock = SimClock::new(DateTime::default());
        let (_guard, db_pool) = db.set_up().await;
        let created_at = sim_clock.now();
        let db_connection = db_pool.connection();
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
        let activity_exec_task = spawn_activity(
            db_pool.clone(),
            test_programs_http_get_activity_builder::TEST_PROGRAMS_HTTP_GET_ACTIVITY,
            sim_clock.clone(),
            TokioSleep,
        )
        .await;

        let workflow_exec_task = spawn_workflow(
            db_pool.clone(),
            test_programs_http_get_workflow_builder::TEST_PROGRAMS_HTTP_GET_WORKFLOW,
            sim_clock.clone(),
            join_next_blocking_strategy,
            fn_registry,
        )
        .await;
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
                retry_exp_backoff: Duration::ZERO,
                max_retries: u32::MAX,
                component_id: ComponentId::dummy_workflow(),
                scheduled_by: None,
            })
            .await
            .unwrap();
        // Check the result.
        let res = assert_matches!(
            db_connection
                .wait_for_finished_result(&execution_id, None)
                .await
                .unwrap(),
            Ok(res) => res
        );
        let val = assert_matches!(res.value(), Some(wast_val) => wast_val);
        let val = assert_matches!(val, WastVal::Result(Ok(Some(val))) => val).deref();
        let val = assert_matches!(val, WastVal::String(val) => val);
        assert_eq!(BODY, val.deref());
        drop(db_connection);
        activity_exec_task.close().await;
        workflow_exec_task.close().await;
        db_pool.close().await.unwrap();
    }

    #[cfg(not(madsim))]
    #[rstest::rstest]
    #[tokio::test]
    async fn http_get_concurrent(
        #[values(db_tests::Database::Memory, db_tests::Database::Sqlite)] db: db_tests::Database,
        #[values(JoinNextBlockingStrategy::Interrupt, JoinNextBlockingStrategy::Await { non_blocking_event_batching: 0}, JoinNextBlockingStrategy::Await { non_blocking_event_batching: 10})]
        join_next_strategy: JoinNextBlockingStrategy,
    ) {
        use crate::activity::activity_worker::tests::spawn_activity;
        use chrono::DateTime;
        use std::ops::Deref;
        use tracing::debug;
        use wiremock::{
            Mock, MockServer, ResponseTemplate,
            matchers::{method, path},
        };
        const BODY: &str = "ok";
        const GET_SUCCESSFUL_CONCURRENTLY_STRESS: FunctionFqn =
            FunctionFqn::new_static_tuple(test_programs_http_get_workflow_builder::exports::testing::http_workflow::workflow::GET_SUCCESSFUL_CONCURRENTLY_STRESS);

        test_utils::set_up();
        let concurrency: u32 = std::env::var("CONCURRENCY")
            .map(|c| c.parse::<u32>())
            .ok()
            .transpose()
            .ok()
            .flatten()
            .unwrap_or(5);
        let sim_clock = SimClock::new(DateTime::default());
        let created_at = sim_clock.now();
        let (_guard, db_pool) = db.set_up().await;
        let db_connection = db_pool.connection();
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

        let activity_exec_task = spawn_activity(
            db_pool.clone(),
            test_programs_http_get_activity_builder::TEST_PROGRAMS_HTTP_GET_ACTIVITY,
            sim_clock.clone(),
            TokioSleep,
        )
        .await;
        let workflow_exec_task = spawn_workflow(
            db_pool.clone(),
            test_programs_http_get_workflow_builder::TEST_PROGRAMS_HTTP_GET_WORKFLOW,
            sim_clock.clone(),
            join_next_strategy,
            fn_registry,
        )
        .await;
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
                retry_exp_backoff: Duration::from_millis(0),
                max_retries: u32::MAX,
                component_id: ComponentId::dummy_workflow(),
                scheduled_by: None,
            })
            .await
            .unwrap();
        // Check the result.
        let res = assert_matches!(
            db_connection
                .wait_for_finished_result(&execution_id, None)
                .await
                .unwrap(),
            Ok(res) => res
        );
        let val = assert_matches!(res.value(), Some(wast_val) => wast_val);
        let val = assert_matches!(val, WastVal::Result(Ok(Some(val))) => val).deref();
        let val = assert_matches!(val, WastVal::List(vec) => vec);
        assert_eq!(concurrency as usize, val.len());
        for val in val {
            let val = assert_matches!(val, WastVal::String(val) => val);
            assert_eq!(BODY, val.deref());
        }
        activity_exec_task.close().await;
        workflow_exec_task.close().await;
        drop(db_connection);
        db_pool.close().await.unwrap();
    }

    #[cfg(not(madsim))]
    #[rstest::rstest]
    #[tokio::test]
    async fn schedule(
        #[values(db_tests::Database::Memory, db_tests::Database::Sqlite)] db: db_tests::Database,
        #[values(JoinNextBlockingStrategy::Interrupt, JoinNextBlockingStrategy::Await { non_blocking_event_batching: 0}, JoinNextBlockingStrategy::Await { non_blocking_event_batching: 10})]
        join_next_strategy: JoinNextBlockingStrategy,
    ) {
        use concepts::prefixed_ulid::ExecutorId;

        const SLEEP_DURATION: Duration = Duration::from_millis(100);
        const ITERATIONS: u8 = 1;
        const FFQN_WORKFLOW_SLEEP_RESCHEDULE_FFQN: FunctionFqn =
            FunctionFqn::new_static_tuple(test_programs_sleep_workflow_builder::exports::testing::sleep_workflow::workflow::RESCHEDULE);
        test_utils::set_up();
        let sim_clock = SimClock::default();
        let (_guard, db_pool) = db.set_up().await;
        let fn_registry = TestingFnRegistry::new_from_components(vec![
            compile_activity(test_programs_sleep_activity_builder::TEST_PROGRAMS_SLEEP_ACTIVITY)
                .await,
            compile_workflow(test_programs_sleep_workflow_builder::TEST_PROGRAMS_SLEEP_WORKFLOW)
                .await,
        ]);
        let worker = compile_workflow_worker(
            test_programs_sleep_workflow_builder::TEST_PROGRAMS_SLEEP_WORKFLOW,
            db_pool.clone(),
            sim_clock.clone(),
            TokioSleep,
            join_next_strategy,
            fn_registry,
        )
        .await;
        let execution_id = ExecutionId::generate();
        let db_connection = db_pool.connection();

        let params = Params::from_json_values(vec![
            json!({"milliseconds": SLEEP_DURATION.as_millis()}),
            json!(ITERATIONS),
        ]);
        db_connection
            .create(CreateRequest {
                created_at: sim_clock.now(),
                execution_id: execution_id.clone(),
                ffqn: FFQN_WORKFLOW_SLEEP_RESCHEDULE_FFQN,
                params,
                parent: None,
                metadata: concepts::ExecutionMetadata::empty(),
                scheduled_at: sim_clock.now(),
                retry_exp_backoff: Duration::ZERO,
                max_retries: u32::MAX,
                component_id: ComponentId::dummy_workflow(),
                scheduled_by: None,
            })
            .await
            .unwrap();
        let exec_task = ExecTask::new(
            worker,
            ExecConfig {
                batch_size: 1,
                lock_expiry: Duration::from_secs(1),
                tick_sleep: TICK_SLEEP,
                component_id: ComponentId::dummy_workflow(),
                task_limiter: None,
            },
            sim_clock.clone(),
            db_pool.clone(),
            Arc::new([FFQN_WORKFLOW_SLEEP_RESCHEDULE_FFQN]),
        );
        // tick + await should mark the first execution finished.
        assert_eq!(
            1,
            exec_task
                .tick_test(sim_clock.now())
                .await
                .unwrap()
                .wait_for_tasks()
                .await
                .unwrap()
        );
        let res = db_pool.connection().get(&execution_id).await.unwrap();
        assert_matches!(
            res.into_finished_result().unwrap(),
            Ok(SupportedFunctionReturnValue::None)
        );
        sim_clock.move_time_forward(SLEEP_DURATION).await;
        // New execution should be pending.
        let mut next_pending = db_pool
            .connection()
            .lock_pending(
                10,
                sim_clock.now(),
                Arc::from([FFQN_WORKFLOW_SLEEP_RESCHEDULE_FFQN]),
                sim_clock.now(),
                ComponentId::dummy_workflow(),
                ExecutorId::generate(),
                sim_clock.now() + Duration::from_secs(1),
            )
            .await
            .unwrap();
        assert_eq!(1, next_pending.len());
        let next_pending = next_pending.pop().unwrap();
        assert!(next_pending.parent.is_none());
        let params = serde_json::to_string(&Params::from_json_values(vec![
            json!({"milliseconds":SLEEP_DURATION.as_millis()}),
            json!(ITERATIONS - 1),
        ]))
        .unwrap();
        assert_eq!(params, serde_json::to_string(&next_pending.params).unwrap());
        drop(exec_task);
        db_pool.close().await.unwrap();
    }

    #[cfg(not(madsim))]
    #[rstest]
    #[tokio::test]
    async fn http_get_fallible_err(
        #[values(Database::Memory, Database::Sqlite)] database: Database,
    ) {
        use crate::activity::activity_worker::tests::spawn_activity;
        use chrono::DateTime;
        use concepts::storage::{
            PendingStateFinished, PendingStateFinishedError, PendingStateFinishedResultKind,
        };
        use std::ops::Deref;

        test_utils::set_up();
        let sim_clock = SimClock::new(DateTime::default());
        let (_guard, db_pool) = database.set_up().await;
        let created_at = sim_clock.now();
        let db_connection = db_pool.connection();
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
        let activity_exec_task = spawn_activity(
            db_pool.clone(),
            test_programs_http_get_activity_builder::TEST_PROGRAMS_HTTP_GET_ACTIVITY,
            sim_clock.clone(),
            TokioSleep,
        )
        .await;

        let workflow_exec_task = spawn_workflow(
            db_pool.clone(),
            test_programs_http_get_workflow_builder::TEST_PROGRAMS_HTTP_GET_WORKFLOW,
            sim_clock.clone(),
            JoinNextBlockingStrategy::Await {
                non_blocking_event_batching: 0,
            },
            fn_registry,
        )
        .await;

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
                retry_exp_backoff: Duration::ZERO,
                max_retries: u32::MAX,
                component_id: ComponentId::dummy_workflow(),
                scheduled_by: None,
            })
            .await
            .unwrap();
        // Check the result.
        let res: SupportedFunctionReturnValue = assert_matches!(
            db_connection
                .wait_for_finished_result(&execution_id, None)
                .await
                .unwrap(),
            Ok(res) => res
        );
        let val = assert_matches!(res.value(), Some(wast_val) => wast_val);
        let val = assert_matches!(val, WastVal::Result(Err(Some(val))) => val).deref();
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

        drop(db_connection);
        activity_exec_task.close().await;
        workflow_exec_task.close().await;
        db_pool.close().await.unwrap();
    }
}
