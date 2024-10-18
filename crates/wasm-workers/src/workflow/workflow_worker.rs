use super::event_history::ApplyError;
use super::workflow_ctx::InterruptRequested;
use super::workflow_ctx::{WorkflowCtx, WorkflowFunctionError};
use crate::workflow::workflow_ctx::WorkerPartialResult;
use crate::WasmFileError;
use crate::NAMESPACE_OBELISK_WITH_COLON;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use concepts::storage::{DbConnection, DbPool};
use concepts::{
    ConfigId, FunctionFqn, FunctionMetadata, PackageIfcFns, ResultParsingError, StrVariant,
};
use concepts::{FunctionRegistry, SupportedFunctionReturnValue};
use executor::worker::{FatalError, WorkerContext, WorkerResult};
use executor::worker::{Worker, WorkerError};
use std::error::Error;
use std::ops::Deref;
use std::path::Path;
use std::time::Duration;
use std::{fmt::Debug, sync::Arc};
use tracing::{debug, error, info, instrument, trace, warn, Span};
use utils::time::{now_tokio_instant, ClockFn};
use utils::wasm_tools::{ExIm, WasmComponent};
use wasmtime::component::{ComponentExportIndex, InstancePre};
use wasmtime::{component::Val, Engine};
use wasmtime::{Store, UpdateDeadline};

/// Defines behavior of the wasm runtime when `HistoryEvent::JoinNextBlocking` is requested.
#[derive(
    Debug, Default, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize, Hash,
)]
pub enum JoinNextBlockingStrategy {
    /// Shut down the current runtime. When the [`JoinSetResponse`] is appended, workflow is reexecuted with a new `RunId`.
    Interrupt,
    /// Keep the execution hot. Worker will poll the database until the execution lock expires.
    #[default]
    Await,
}

#[derive(Clone, Debug)]
pub struct WorkflowConfig {
    pub config_id: ConfigId,
    pub join_next_blocking_strategy: JoinNextBlockingStrategy,
    pub non_blocking_event_batching: u32,
}

pub struct WorkflowWorkerCompiled<C: ClockFn> {
    config: WorkflowConfig,
    engine: Arc<Engine>,
    clock_fn: C,
    wasm_component: WasmComponent,
}

pub struct WorkflowWorkerLinked<C: ClockFn, DB: DbConnection, P: DbPool<DB>> {
    config: WorkflowConfig,
    engine: Arc<Engine>,
    exim: ExIm,
    clock_fn: C,
    exported_ffqn_to_index: hashbrown::HashMap<FunctionFqn, ComponentExportIndex>,
    fn_registry: Arc<dyn FunctionRegistry>,
    instance_pre: InstancePre<WorkflowCtx<C, DB, P>>,
}

#[derive(Clone)]
pub struct WorkflowWorker<C: ClockFn, DB: DbConnection, P: DbPool<DB>> {
    config: WorkflowConfig,
    engine: Arc<Engine>,
    exim: ExIm,
    db_pool: P,
    clock_fn: C,
    exported_ffqn_to_index: hashbrown::HashMap<FunctionFqn, ComponentExportIndex>,
    fn_registry: Arc<dyn FunctionRegistry>,
    instance_pre: InstancePre<WorkflowCtx<C, DB, P>>,
}

impl<C: ClockFn> WorkflowWorkerCompiled<C> {
    #[tracing::instrument(skip_all, fields(%config.config_id), err)]
    pub fn new_with_config(
        wasm_path: impl AsRef<Path>,
        config: WorkflowConfig,
        engine: Arc<Engine>,
        clock_fn: C,
    ) -> Result<Self, WasmFileError> {
        let wasm_path = wasm_path.as_ref();
        let wasm_component =
            WasmComponent::new(wasm_path, &engine).map_err(WasmFileError::DecodeError)?;
        Ok(Self {
            config,
            engine,
            clock_fn,
            wasm_component,
        })
    }

    #[instrument(skip_all, fields(config_id = %self.config.config_id))]
    pub fn link<DB: DbConnection, P: DbPool<DB>>(
        self,
        fn_registry: Arc<dyn FunctionRegistry>,
    ) -> Result<WorkflowWorkerLinked<C, DB, P>, WasmFileError> {
        let mut linker = wasmtime::component::Linker::new(&self.engine);

        // Link obelisk: host-activities and log
        WorkflowCtx::add_to_linker(&mut linker)?;

        // Mock imported functions
        for import in fn_registry.all_exports() {
            if import
                .ifc_fqn
                .deref()
                .starts_with(NAMESPACE_OBELISK_WITH_COLON)
            {
                warn!(ifc_fqn = %import.ifc_fqn, "Skipping mock of reserved interface");
                continue;
            }
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
                            let ffqn = ffqn.clone();
                            Box::new(async move {
                                Ok(store_ctx
                                    .data_mut()
                                    .call_imported_fn(ffqn, params, results)
                                    .await?)
                            })
                        }
                    });
                    if let Err(err) = res {
                        if err.to_string() == format!("import `{function_name}` not found") {
                            // FIXME: Add test for error message stability
                            debug!("Skipping mocking of {ffqn}");
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
            exported_ffqn_to_index,
            fn_registry,
            instance_pre,
        })
    }

    pub fn exported_functions(&self) -> &[FunctionMetadata] {
        &self.wasm_component.exim.exports_flat
    }

    pub fn exports_hierarchy(&self) -> &[PackageIfcFns] {
        &self.wasm_component.exim.exports_hierarchy
    }

    pub fn imported_functions(&self) -> &[FunctionMetadata] {
        &self.wasm_component.exim.imports_flat
    }
}

impl<C: ClockFn, DB: DbConnection, P: DbPool<DB>> WorkflowWorkerLinked<C, DB, P> {
    pub fn into_worker(self, db_pool: P) -> WorkflowWorker<C, DB, P> {
        WorkflowWorker {
            config: self.config,
            engine: self.engine,
            exim: self.exim,
            db_pool,
            clock_fn: self.clock_fn,
            exported_ffqn_to_index: self.exported_ffqn_to_index,
            fn_registry: self.fn_registry,
            instance_pre: self.instance_pre,
        }
    }
}

enum RunError<C: ClockFn + 'static, DB: DbConnection + 'static, P: DbPool<DB> + 'static> {
    ResultParsingError(ResultParsingError, WorkflowCtx<C, DB, P>),
    FunctionCall(Box<dyn Error + Send + Sync>, WorkflowCtx<C, DB, P>),
    WorkerPartialResult(WorkerPartialResult, WorkflowCtx<C, DB, P>),
}

enum WorkerResultRefactored<C: ClockFn, DB: DbConnection, P: DbPool<DB>> {
    Ok(SupportedFunctionReturnValue, WorkflowCtx<C, DB, P>),
    FatalError(FatalError, WorkflowCtx<C, DB, P>),
    Retryable(WorkerResult),
}

type CallFuncResult<C, DB, P> =
    Result<(SupportedFunctionReturnValue, WorkflowCtx<C, DB, P>), RunError<C, DB, P>>;

impl<C: ClockFn + 'static, DB: DbConnection + 'static, P: DbPool<DB> + 'static>
    WorkflowWorker<C, DB, P>
{
    async fn prepare_func(
        &self,
        ctx: WorkerContext,
        interrupt_on_timeout_container: Arc<std::sync::Mutex<Option<InterruptRequested>>>,
    ) -> Result<
        (
            Store<WorkflowCtx<C, DB, P>>,
            wasmtime::component::Func,
            Arc<[Val]>,
        ),
        WorkerError,
    > {
        let version_at_start = ctx.version.clone();
        let fn_export_index = self
            .exported_ffqn_to_index
            .get(&ctx.ffqn)
            .expect("executor only calls `run` with ffqns that are exported");
        let seed = ctx.execution_id.random_part();
        let workflow_ctx = WorkflowCtx::new(
            ctx.execution_id,
            ctx.event_history,
            ctx.responses,
            seed,
            self.clock_fn.clone(),
            self.config.join_next_blocking_strategy,
            self.db_pool.clone(),
            ctx.version,
            ctx.execution_deadline,
            self.config.non_blocking_event_batching,
            interrupt_on_timeout_container,
            self.fn_registry.clone(),
            ctx.worker_span,
            ctx.topmost_parent,
        );
        let mut store = Store::new(&self.engine, workflow_ctx);
        let instance = match self.instance_pre.instantiate_async(&mut store).await {
            Ok(instance) => instance,
            Err(err) => {
                let reason = err.to_string();
                let version = store.into_data().version;
                if reason.starts_with("maximum concurrent") {
                    return Err(WorkerError::LimitReached(reason, version));
                }
                return Err(WorkerError::IntermittentError {
                    reason: StrVariant::Arc(Arc::from(format!("cannot instantiate - {err}"))),
                    err: Some(err.into()),
                    version,
                });
            }
        };
        store.epoch_deadline_callback(|_store_ctx| Ok(UpdateDeadline::Yield(1)));

        let func = instance
            .get_func(&mut store, fn_export_index)
            .expect("exported function must be found");

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
                warn!("Ignoring post return error: {post_return_err:?}");
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
                // guest panic exits here.
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
                    Err(RunError::FunctionCall(err.into(), workflow_ctx))
                }
            }
        }
    }

    async fn race_func_with_timeout(
        &self,
        store: Store<WorkflowCtx<C, DB, P>>,
        func: wasmtime::component::Func,
        params: Arc<[Val]>,
        interrupt_on_timeout_container: Arc<std::sync::Mutex<Option<InterruptRequested>>>,
        worker_span: Span,
        execution_deadline: DateTime<Utc>,
    ) -> WorkerResult {
        let started_at = self.clock_fn.now();
        let deadline_duration = (execution_deadline - started_at).to_std().unwrap();
        let stopwatch = now_tokio_instant(); // Not using `clock_fn` here is ok, value is only used for log reporting.
        tokio::select! { // future's liveness: Dropping the loser immediately.
            res = Self::race_func_internal(store, func, params, &worker_span, stopwatch, deadline_duration, execution_deadline) => {
                res
            },
            () = tokio::time::sleep(deadline_duration) => {
                // Not flushing the workflow_ctx as it would introduce locking.
                // Not closing the join sets, workflow MUST be retried.
                if interrupt_on_timeout_container.lock().unwrap().take().is_some() {
                    worker_span.in_scope(||
                        info!(duration = ?stopwatch.elapsed(), ?deadline_duration, %execution_deadline, now = %self.clock_fn.now(), "Interrupt requested")
                    );
                    WorkerResult::DbUpdatedByWorker
                } else {
                    worker_span.in_scope(||
                        info!(duration = ?stopwatch.elapsed(), ?deadline_duration, %execution_deadline, now = %self.clock_fn.now(), "Timed out")
                    );
                    WorkerResult::Err(WorkerError::IntermittentTimeout)
                }
            }
        }
    }

    async fn race_func_internal(
        store: Store<WorkflowCtx<C, DB, P>>,
        func: wasmtime::component::Func,
        params: Arc<[Val]>,
        worker_span: &Span,
        stopwatch: tokio::time::Instant,
        deadline_duration: Duration,
        execution_deadline: DateTime<Utc>,
    ) -> WorkerResult {
        let res = Self::call_func(store, func, params).await;
        let worker_result_refactored = Self::convert_result(
            res,
            worker_span,
            stopwatch,
            deadline_duration,
            execution_deadline,
        )
        .await;
        match worker_result_refactored {
            WorkerResultRefactored::Ok(res, mut workflow_ctx) => {
                match Self::close_join_sets(&mut workflow_ctx).await {
                    Err(worker_result) => worker_result,
                    Ok(()) => WorkerResult::Ok(res, workflow_ctx.version),
                }
            }
            WorkerResultRefactored::FatalError(err, mut workflow_ctx) => {
                match Self::close_join_sets(&mut workflow_ctx).await {
                    Err(worker_result) => worker_result,
                    Ok(()) => WorkerResult::Err(WorkerError::FatalError(err, workflow_ctx.version)),
                }
            }
            WorkerResultRefactored::Retryable(retryable) => retryable,
        }
    }

    async fn convert_result(
        res: CallFuncResult<C, DB, P>,
        worker_span: &Span,
        stopwatch: tokio::time::Instant,
        deadline_duration: Duration,
        execution_deadline: DateTime<Utc>,
    ) -> WorkerResultRefactored<C, DB, P> {
        match res {
            Ok((supported_result, mut workflow_ctx)) => {
                worker_span.in_scope(||
                    info!(duration = ?stopwatch.elapsed(), ?deadline_duration, %execution_deadline, "Finished")
                );
                if let Err(db_err) = workflow_ctx.flush().await {
                    worker_span.in_scope(|| error!("Database error: {db_err}"));
                    return WorkerResultRefactored::Retryable(WorkerResult::Err(
                        WorkerError::DbError(db_err),
                    ));
                }
                WorkerResultRefactored::Ok(supported_result, workflow_ctx)
            }
            Err(RunError::FunctionCall(err, mut workflow_ctx)) => {
                if let Err(db_err) = workflow_ctx.flush().await {
                    worker_span.in_scope(||
                        error!("Database flush error: {db_err:?} while handling FunctionCall error: {err:?}")
                    );
                    return WorkerResultRefactored::Retryable(WorkerResult::Err(
                        WorkerError::DbError(db_err),
                    ));
                }
                let version = workflow_ctx.version;
                let err = WorkerError::IntermittentError {
                    reason: StrVariant::Arc(Arc::from(format!(
                        "uncategorized function call error - {err:?}"
                    ))),
                    err: Some(err),
                    version,
                };
                worker_span.in_scope(||
                    info!(duration = ?stopwatch.elapsed(), ?deadline_duration, %execution_deadline, "Finished with an intermittent error - {err:?}")
                );
                WorkerResultRefactored::Retryable(WorkerResult::Err(err))
            }
            Err(RunError::WorkerPartialResult(worker_partial_result, mut workflow_ctx)) => {
                if let Err(db_err) = workflow_ctx.flush().await {
                    worker_span.in_scope(||
                        error!("Database flush error: {db_err:?} while handling WorkerPartialResult: {worker_partial_result:?}")
                    );
                    return WorkerResultRefactored::Retryable(WorkerResult::Err(
                        WorkerError::DbError(db_err),
                    ));
                }
                match worker_partial_result {
                    WorkerPartialResult::FatalError(err, _version) => {
                        worker_span.in_scope(|| info!(duration = ?stopwatch.elapsed(), ?deadline_duration, %execution_deadline, "Finished with a worker error: {err:?}"));
                        WorkerResultRefactored::FatalError(err, workflow_ctx)
                    }
                    WorkerPartialResult::InterruptRequested => {
                        worker_span.in_scope(|| info!(duration = ?stopwatch.elapsed(), ?deadline_duration, %execution_deadline, "Interrupt requested"));
                        WorkerResultRefactored::Retryable(WorkerResult::DbUpdatedByWorker)
                    }
                    WorkerPartialResult::DbError(db_err) => WorkerResultRefactored::Retryable(
                        WorkerResult::Err(WorkerError::DbError(db_err)),
                    ),
                }
            }
            Err(RunError::ResultParsingError(err, mut workflow_ctx)) => {
                if let Err(db_err) = workflow_ctx.flush().await {
                    worker_span.in_scope(|| error!("Database error: {db_err}"));
                    return WorkerResultRefactored::Retryable(WorkerResult::Err(
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
        loop {
            match workflow_ctx.close_opened_join_set().await {
                Ok(None) => return Ok(()),
                Err(apply_err) => {
                    match apply_err {
                        ApplyError::NondeterminismDetected(reason) => {
                            return Err(WorkerResult::Err(WorkerError::FatalError(
                                FatalError::NonDeterminismDetected(reason),
                                workflow_ctx.version.clone(),
                            )));
                        }
                        ApplyError::ChildExecutionError(_) => {
                            unreachable!("direct call to child execution does not happen on closing join sets")
                        }
                        ApplyError::InterruptRequested => {
                            return Err(WorkerResult::DbUpdatedByWorker);
                        }
                        ApplyError::DbError(db_error) => {
                            return Err(WorkerResult::Err(WorkerError::DbError(db_error)));
                        }
                    };
                }
                Ok(Some(())) => {}
            }
        }
    }
}

#[async_trait]
impl<C: ClockFn + 'static, DB: DbConnection + 'static, P: DbPool<DB> + 'static> Worker
    for WorkflowWorker<C, DB, P>
{
    fn exported_functions(&self) -> &[FunctionMetadata] {
        &self.exim.exports_flat
    }

    fn imported_functions(&self) -> &[FunctionMetadata] {
        &self.exim.imports_flat
    }

    async fn run(&self, ctx: WorkerContext) -> WorkerResult {
        trace!("Params: {params:?}", params = ctx.params);
        let interrupt_on_timeout_container = Arc::new(std::sync::Mutex::new(None));
        let worker_span = ctx.worker_span.clone();
        let execution_deadline = ctx.execution_deadline;
        let (store, func, params) = match self
            .prepare_func(ctx, interrupt_on_timeout_container.clone())
            .await
        {
            Ok(ok) => ok,
            Err(err) => return WorkerResult::Err(err), // errors here cannot affect joinset closing, as they happen before fn invocation
        };
        self.race_func_with_timeout(
            store,
            func,
            params,
            interrupt_on_timeout_container,
            worker_span,
            execution_deadline,
        )
        .await
    }
}

#[cfg(test)]
pub(crate) mod tests {
    // TODO: test timeouts
    use super::*;
    use crate::{
        activity::activity_worker::tests::{
            compile_activity, spawn_activity_fibo, wasm_file_name, FIBO_10_INPUT, FIBO_10_OUTPUT,
        },
        engines::{EngineConfig, Engines},
        tests::{fn_registry_dummy, TestingFnRegistry},
    };
    use assert_matches::assert_matches;
    use concepts::{
        prefixed_ulid::ExecutorId,
        storage::{wait_for_pending_state_fn, CreateRequest, DbConnection, PendingState},
        ConfigIdType, ImportableType,
    };
    use concepts::{ExecutionId, Params};
    use db_tests::Database;
    use executor::{
        executor::{ExecConfig, ExecTask, ExecutorTaskHandle},
        expired_timers_watcher,
    };
    use rstest::rstest;
    use serde_json::json;
    use std::time::Duration;
    use test_utils::sim_clock::SimClock;
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
    const HTTP_GET_SUCCESSFUL_WORKFLOW_FFQN: FunctionFqn = FunctionFqn::new_static_tuple(test_programs_http_get_workflow_builder::exports::testing::http_workflow::workflow::GET_SUCCESSFUL);

    pub(crate) async fn compile_workflow(
        wasm_path: &str,
    ) -> (WasmComponent, ConfigId, ImportableType) {
        let engine = Engines::get_workflow_engine(EngineConfig::on_demand_testing().await).unwrap();
        let config_id = ConfigId::new(
            ConfigIdType::Workflow,
            wasm_file_name(wasm_path),
            StrVariant::Static("dummy hash"),
        )
        .unwrap();
        (
            WasmComponent::new(wasm_path, &engine).unwrap(),
            config_id,
            ImportableType::Workflow,
        )
    }

    async fn spawn_workflow<DB: DbConnection + 'static, P: DbPool<DB> + 'static>(
        db_pool: P,
        wasm_path: &'static str,
        clock_fn: impl ClockFn + 'static,
        join_next_blocking_strategy: JoinNextBlockingStrategy,
        non_blocking_event_batching: u32,
        fn_registry: Arc<dyn FunctionRegistry>,
    ) -> ExecutorTaskHandle {
        let workflow_engine =
            Engines::get_workflow_engine(EngineConfig::on_demand_testing().await).unwrap();
        let config_id = ConfigId::new(
            ConfigIdType::Workflow,
            wasm_file_name(wasm_path),
            StrVariant::Static("dummy hash"),
        )
        .unwrap();
        let worker = Arc::new(
            WorkflowWorkerCompiled::new_with_config(
                wasm_path,
                WorkflowConfig {
                    config_id: config_id.clone(),
                    join_next_blocking_strategy,
                    non_blocking_event_batching,
                },
                workflow_engine,
                clock_fn.clone(),
            )
            .unwrap()
            .link(fn_registry)
            .unwrap()
            .into_worker(db_pool.clone()),
        );
        info!("Instantiated worker");
        let exec_config = ExecConfig {
            batch_size: 1,
            lock_expiry: Duration::from_secs(3),
            tick_sleep: TICK_SLEEP,
            config_id,
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
        non_blocking_event_batching: u32,
        fn_registry: Arc<dyn FunctionRegistry>,
    ) -> ExecutorTaskHandle {
        spawn_workflow(
            db_pool,
            test_programs_fibo_workflow_builder::TEST_PROGRAMS_FIBO_WORKFLOW,
            clock_fn,
            join_next_blocking_strategy,
            non_blocking_event_batching,
            fn_registry,
        )
        .await
    }

    #[rstest]
    #[tokio::test]
    async fn fibo_workflow_should_schedule_fibo_activity_mem(
        #[values(JoinNextBlockingStrategy::Interrupt, JoinNextBlockingStrategy::Await)]
        join_next_blocking_strategy: JoinNextBlockingStrategy,
        #[values(0, 10)] batching: u32,
    ) {
        let sim_clock = SimClock::default();
        let (_guard, db_pool) = Database::Memory.set_up().await;
        fibo_workflow_should_schedule_fibo_activity(
            db_pool.clone(),
            sim_clock,
            join_next_blocking_strategy,
            batching,
        )
        .await;
        db_pool.close().await.unwrap();
    }

    #[cfg(not(madsim))]
    #[rstest]
    #[tokio::test]
    async fn fibo_workflow_should_schedule_fibo_activity_sqlite(
        #[values(JoinNextBlockingStrategy::Interrupt, JoinNextBlockingStrategy::Await)]
        join_next_blocking_strategy: JoinNextBlockingStrategy,
        #[values(0, 10)] batching: u32,
    ) {
        let sim_clock = SimClock::default();
        let (_guard, db_pool) = Database::Sqlite.set_up().await;
        fibo_workflow_should_schedule_fibo_activity(
            db_pool.clone(),
            sim_clock,
            join_next_blocking_strategy,
            batching,
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
        batching: u32,
    ) {
        const INPUT_ITERATIONS: u32 = 1;
        let _guard = test_utils::set_up();
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
            batching,
            fn_registry,
        )
        .await;
        // Create an execution.
        let execution_id = ExecutionId::from_parts(0, 0);
        let created_at = sim_clock.now();
        let db_connection = db_pool.connection();

        let params = Params::from_json_value(json!([FIBO_10_INPUT, INPUT_ITERATIONS])).unwrap();
        db_connection
            .create(CreateRequest {
                created_at,
                execution_id,
                ffqn: FIBO_WORKFLOW_FFQN,
                params,
                parent: None,
                metadata: concepts::ExecutionMetadata::empty(),
                scheduled_at: created_at,
                retry_exp_backoff: Duration::ZERO,
                max_retries: 0,
                config_id: ConfigId::dummy_activity(),
                return_type: None,
                topmost_parent: execution_id,
            })
            .await
            .unwrap();
        info!("Should end as BlockedByJoinSet");
        wait_for_pending_state_fn(
            &db_connection,
            execution_id,
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
        let activity_exec_task = spawn_activity_fibo(db_pool.clone(), sim_clock.clone()).await;

        let res = db_connection
            .wait_for_finished_result(execution_id, None)
            .await
            .unwrap()
            .unwrap();
        let res = assert_matches!(res, SupportedFunctionReturnValue::Infallible(val) => val);
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
        let _guard = test_utils::set_up();
        let fn_registry = fn_registry_dummy(&[]);
        spawn_workflow_fibo(
            db_pool.clone(),
            sim_clock.clone(),
            JoinNextBlockingStrategy::Interrupt,
            0,
            fn_registry,
        )
        .await;
        db_pool.close().await.unwrap();
    }

    pub(crate) async fn get_workflow_worker<
        C: ClockFn,
        DB: DbConnection + 'static,
        P: DbPool<DB> + 'static,
    >(
        path: &str,
        db_pool: P,
        clock_fn: C,
        join_next_blocking_strategy: JoinNextBlockingStrategy,
        non_blocking_event_batching: u32,
        fn_registry: Arc<dyn FunctionRegistry>,
    ) -> Arc<WorkflowWorker<C, DB, P>> {
        Arc::new(
            WorkflowWorkerCompiled::new_with_config(
                path,
                WorkflowConfig {
                    config_id: ConfigId::dummy_activity(),
                    join_next_blocking_strategy,
                    non_blocking_event_batching,
                },
                Engines::get_workflow_engine(EngineConfig::on_demand_testing().await).unwrap(),
                clock_fn,
            )
            .unwrap()
            .link(fn_registry)
            .unwrap()
            .into_worker(db_pool),
        )
    }

    pub(crate) async fn spawn_workflow_sleep<
        DB: DbConnection + 'static,
        P: DbPool<DB> + 'static,
    >(
        db_pool: P,
        clock_fn: impl ClockFn + 'static,
        join_next_blocking_strategy: JoinNextBlockingStrategy,
        non_blocking_event_batching: u32,
        fn_registry: Arc<dyn FunctionRegistry>,
    ) -> ExecutorTaskHandle {
        let worker = get_workflow_worker(
            test_programs_sleep_workflow_builder::TEST_PROGRAMS_SLEEP_WORKFLOW,
            db_pool.clone(),
            clock_fn.clone(),
            join_next_blocking_strategy,
            non_blocking_event_batching,
            fn_registry,
        )
        .await;
        let exec_config = ExecConfig {
            batch_size: 1,
            lock_expiry: Duration::from_secs(1),
            tick_sleep: TICK_SLEEP,
            config_id: ConfigId::dummy_activity(),
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

    #[rstest]
    #[tokio::test]
    async fn sleep_should_be_persisted_after_executor_restart(
        #[values(JoinNextBlockingStrategy::Interrupt, JoinNextBlockingStrategy::Await)]
        join_next_blocking_strategy: JoinNextBlockingStrategy,
        #[values(0, 10)] batching: u32,
    ) {
        const SLEEP_MILLIS: u32 = 100;
        let _guard = test_utils::set_up();
        let sim_clock = SimClock::default();
        let (_guard, db_pool) = Database::Memory.set_up().await;

        let fn_registry = TestingFnRegistry::new_from_components(vec![
            compile_activity(test_programs_sleep_activity_builder::TEST_PROGRAMS_SLEEP_ACTIVITY)
                .await,
            compile_workflow(test_programs_sleep_workflow_builder::TEST_PROGRAMS_SLEEP_WORKFLOW)
                .await,
        ]);

        let workflow_exec_task = spawn_workflow_sleep(
            db_pool.clone(),
            sim_clock.clone(),
            join_next_blocking_strategy,
            batching,
            fn_registry.clone(),
        )
        .await;
        let timers_watcher_task = expired_timers_watcher::spawn_new(
            db_pool.clone(),
            expired_timers_watcher::TimersWatcherConfig {
                tick_sleep: TICK_SLEEP,
                clock_fn: sim_clock.clone(),
                leeway: Duration::ZERO,
            },
        );
        let execution_id = ExecutionId::generate();
        let db_connection = db_pool.connection();
        let params = Params::from_json_value(json!([{"milliseconds":SLEEP_MILLIS}])).unwrap();
        db_connection
            .create(CreateRequest {
                created_at: sim_clock.now(),
                execution_id,
                ffqn: SLEEP_HOST_ACTIVITY_FFQN,
                params,
                parent: None,
                metadata: concepts::ExecutionMetadata::empty(),
                scheduled_at: sim_clock.now(),
                retry_exp_backoff: Duration::ZERO,
                max_retries: 0,
                config_id: ConfigId::dummy_activity(),
                return_type: None,
                topmost_parent: execution_id,
            })
            .await
            .unwrap();

        wait_for_pending_state_fn(
            &db_connection,
            execution_id,
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

        workflow_exec_task.close().await;
        sim_clock
            .move_time_forward(Duration::from_millis(u64::from(SLEEP_MILLIS)))
            .await;
        // Restart worker
        let workflow_exec_task = spawn_workflow_sleep(
            db_pool.clone(),
            sim_clock.clone(),
            join_next_blocking_strategy,
            batching,
            fn_registry,
        )
        .await;
        let res = db_connection
            .wait_for_finished_result(execution_id, None)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(SupportedFunctionReturnValue::None, res);
        drop(db_connection);
        workflow_exec_task.close().await;
        timers_watcher_task.close().await;
        db_pool.close().await.unwrap();
    }

    #[cfg(not(madsim))]
    #[rstest]
    #[tokio::test]
    async fn http_get(
        #[values(JoinNextBlockingStrategy::Interrupt, JoinNextBlockingStrategy::Await)]
        join_next_blocking_strategy: JoinNextBlockingStrategy,
        #[values(0, 10)] batching: u32,
    ) {
        use crate::activity::activity_worker::tests::spawn_activity;
        use chrono::DateTime;
        use std::ops::Deref;
        use wiremock::{
            matchers::{method, path},
            Mock, MockServer, ResponseTemplate,
        };
        const BODY: &str = "ok";

        test_utils::set_up();
        let sim_clock = SimClock::new(DateTime::default());
        let (_guard, db_pool) = Database::Memory.set_up().await;
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
        )
        .await;

        let workflow_exec_task = spawn_workflow(
            db_pool.clone(),
            test_programs_http_get_workflow_builder::TEST_PROGRAMS_HTTP_GET_WORKFLOW,
            sim_clock.clone(),
            join_next_blocking_strategy,
            batching,
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
        let params = Params::from_json_value(json!([url])).unwrap();
        // Create an execution.
        let execution_id = ExecutionId::generate();
        db_connection
            .create(CreateRequest {
                created_at,
                execution_id,
                ffqn: HTTP_GET_SUCCESSFUL_WORKFLOW_FFQN,
                params,
                parent: None,
                metadata: concepts::ExecutionMetadata::empty(),
                scheduled_at: created_at,
                retry_exp_backoff: Duration::ZERO,
                max_retries: 0,
                config_id: ConfigId::dummy_activity(),
                return_type: None,
                topmost_parent: execution_id,
            })
            .await
            .unwrap();
        // Check the result.
        let res = assert_matches!(
            db_connection
                .wait_for_finished_result(execution_id, None)
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
        #[values(JoinNextBlockingStrategy::Interrupt, JoinNextBlockingStrategy::Await)]
        join_next_strategy: JoinNextBlockingStrategy,
        #[values(0, 10)] batching: u32,
    ) {
        use crate::activity::activity_worker::tests::spawn_activity;
        use chrono::DateTime;
        use std::ops::Deref;
        use wiremock::{
            matchers::{method, path},
            Mock, MockServer, ResponseTemplate,
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
        )
        .await;
        let workflow_exec_task = spawn_workflow(
            db_pool.clone(),
            test_programs_http_get_workflow_builder::TEST_PROGRAMS_HTTP_GET_WORKFLOW,
            sim_clock.clone(),
            join_next_strategy,
            batching,
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
        let params = Params::from_json_value(json!([url, concurrency])).unwrap();
        // Create an execution.
        let execution_id = ExecutionId::generate();

        db_connection
            .create(CreateRequest {
                created_at,
                execution_id,
                ffqn: GET_SUCCESSFUL_CONCURRENTLY_STRESS,
                params,
                parent: None,
                metadata: concepts::ExecutionMetadata::empty(),
                scheduled_at: created_at,
                retry_exp_backoff: Duration::from_millis(0),
                max_retries: concurrency - 1, // response can conflict with next ChildExecutionRequest
                config_id: ConfigId::dummy_activity(),
                return_type: None,
                topmost_parent: execution_id,
            })
            .await
            .unwrap();
        // Check the result.
        let res = assert_matches!(
            db_connection
                .wait_for_finished_result(execution_id, None)
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
        #[values(JoinNextBlockingStrategy::Interrupt, JoinNextBlockingStrategy::Await)]
        join_next_strategy: JoinNextBlockingStrategy,
        #[values(0, 10)] batching: u32,
    ) {
        use concepts::prefixed_ulid::ExecutorId;

        const SLEEP_DURATION: Duration = Duration::from_millis(100);
        const ITERATIONS: u8 = 1;
        const RESCHEDULE_FFQN: FunctionFqn =
            FunctionFqn::new_static_tuple(test_programs_sleep_workflow_builder::exports::testing::sleep_workflow::workflow::RESCHEDULE);
        let _guard = test_utils::set_up();
        let sim_clock = SimClock::default();
        let (_guard, db_pool) = db.set_up().await;
        let fn_registry = TestingFnRegistry::new_from_components(vec![
            compile_activity(test_programs_sleep_activity_builder::TEST_PROGRAMS_SLEEP_ACTIVITY)
                .await,
            compile_workflow(test_programs_sleep_workflow_builder::TEST_PROGRAMS_SLEEP_WORKFLOW)
                .await,
        ]);
        let worker = get_workflow_worker(
            test_programs_sleep_workflow_builder::TEST_PROGRAMS_SLEEP_WORKFLOW,
            db_pool.clone(),
            sim_clock.clone(),
            join_next_strategy,
            batching,
            fn_registry,
        )
        .await;
        let execution_id = ExecutionId::generate();
        let db_connection = db_pool.connection();

        let params = Params::from_json_value(
            json!([{"milliseconds": SLEEP_DURATION.as_millis()}, ITERATIONS]),
        )
        .unwrap();
        db_connection
            .create(CreateRequest {
                created_at: sim_clock.now(),
                execution_id,
                ffqn: RESCHEDULE_FFQN,
                params,
                parent: None,
                metadata: concepts::ExecutionMetadata::empty(),
                scheduled_at: sim_clock.now(),
                retry_exp_backoff: Duration::ZERO,
                max_retries: 0,
                config_id: ConfigId::dummy_activity(),
                return_type: None,
                topmost_parent: execution_id,
            })
            .await
            .unwrap();
        let exec_task = ExecTask::new(
            worker,
            ExecConfig {
                batch_size: 1,
                lock_expiry: Duration::from_secs(1),
                tick_sleep: TICK_SLEEP,
                config_id: ConfigId::dummy_activity(),
                task_limiter: None,
            },
            sim_clock.clone(),
            db_pool.clone(),
            Arc::new([RESCHEDULE_FFQN]),
        );
        // tick2 + await should mark the first execution finished.
        assert_eq!(
            1,
            exec_task
                .tick2(sim_clock.now())
                .await
                .unwrap()
                .wait_for_tasks()
                .await
                .unwrap()
        );
        let res = db_pool.connection().get(execution_id).await.unwrap();
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
                Arc::from([RESCHEDULE_FFQN]),
                sim_clock.now(),
                ConfigId::dummy_activity(),
                ExecutorId::generate(),
                sim_clock.now() + Duration::from_secs(1),
            )
            .await
            .unwrap();
        assert_eq!(1, next_pending.len());
        let next_pending = next_pending.pop().unwrap();
        assert!(next_pending.parent.is_none());
        let params = serde_json::to_string(
            &Params::from_json_value(
                json!([{"milliseconds":SLEEP_DURATION.as_millis()}, ITERATIONS - 1]),
            )
            .unwrap(),
        )
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
        )
        .await;

        let workflow_exec_task = spawn_workflow(
            db_pool.clone(),
            test_programs_http_get_workflow_builder::TEST_PROGRAMS_HTTP_GET_WORKFLOW,
            sim_clock.clone(),
            JoinNextBlockingStrategy::Await,
            0,
            fn_registry,
        )
        .await;

        let url = "http://";
        let params = Params::from_json_value(json!([url])).unwrap();
        // Create an execution.
        let execution_id = ExecutionId::generate();
        db_connection
            .create(CreateRequest {
                created_at,
                execution_id,
                ffqn: HTTP_GET_SUCCESSFUL_WORKFLOW_FFQN,
                params,
                parent: None,
                metadata: concepts::ExecutionMetadata::empty(),
                scheduled_at: created_at,
                retry_exp_backoff: Duration::ZERO,
                max_retries: 0,
                config_id: ConfigId::dummy_activity(),
                return_type: None,
                topmost_parent: execution_id,
            })
            .await
            .unwrap();
        // Check the result.
        let res: SupportedFunctionReturnValue = assert_matches!(
            db_connection
                .wait_for_finished_result(execution_id, None)
                .await
                .unwrap(),
            Ok(res) => res
        );
        let val = assert_matches!(res.value(), Some(wast_val) => wast_val);
        let val = assert_matches!(val, WastVal::Result(Err(Some(val))) => val).deref();
        let val = assert_matches!(val, WastVal::String(val) => val);
        assert_eq!("empty host", val);

        let pending_state = db_connection.get_pending_state(execution_id).await.unwrap();
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
