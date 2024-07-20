use crate::workflow_ctx::{FunctionError, WorkflowCtx};
use crate::WasmFileError;
use async_trait::async_trait;
use concepts::storage::{DbConnection, DbPool};
use concepts::{ComponentConfigHash, FunctionFqn, FunctionMetadata, StrVariant};
use concepts::{FunctionRegistry, SupportedFunctionResult};
use executor::worker::{FatalError, WorkerContext, WorkerResult};
use executor::worker::{Worker, WorkerError};
use std::error::Error;
use std::ops::Deref;
use std::path::Path;
use std::time::Duration;
use std::{fmt::Debug, sync::Arc};
use tracing::{debug, info, trace};
use utils::time::{now_tokio_instant, ClockFn};
use utils::wasm_tools::{ExIm, WasmComponent};
use wasmtime::{component::Val, Engine};
use wasmtime::{Store, UpdateDeadline};

/// Defines behavior of the wasm runtime when `HistoryEvent::JoinNextBlocking` is requested.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum JoinNextBlockingStrategy {
    /// Shut down the current runtime. When the [`JoinSetResponse`] is appended, workflow is reexecuted with a new `RunId`.
    Interrupt,
    /// Keep the execution hot. Worker will poll the database until the execution lock expires.
    #[default]
    Await,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, serde::Serialize, serde::Deserialize)]
pub enum NonBlockingEventBatching {
    #[default]
    Enabled,
    Disabled,
}

impl From<bool> for NonBlockingEventBatching {
    fn from(value: bool) -> Self {
        if value {
            NonBlockingEventBatching::Enabled
        } else {
            NonBlockingEventBatching::Disabled
        }
    }
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct WorkflowConfig {
    pub config_id: ComponentConfigHash,
    pub join_next_blocking_strategy: JoinNextBlockingStrategy,
    pub child_retry_exp_backoff: Duration,
    pub child_max_retries: u32,
    pub non_blocking_event_batching: NonBlockingEventBatching,
}

#[derive(Clone)]
pub struct WorkflowWorker<C: ClockFn, DB: DbConnection, P: DbPool<DB>> {
    config: WorkflowConfig,
    engine: Arc<Engine>,
    linker: wasmtime::component::Linker<WorkflowCtx<C, DB, P>>,
    component: wasmtime::component::Component,
    exim: ExIm,
    db_pool: P,
    clock_fn: C,
    fn_registry: Arc<dyn FunctionRegistry>,
}

pub(crate) const HOST_ACTIVITY_IFC_STRING: &str = "obelisk:workflow/host-activities";

impl<C: ClockFn, DB: DbConnection, P: DbPool<DB>> WorkflowWorker<C, DB, P> {
    #[tracing::instrument(skip_all, fields(config_id = %config.config_id))]
    pub fn new_with_config(
        wasm_path: impl AsRef<Path>,
        config: WorkflowConfig,
        engine: Arc<Engine>,
        db_pool: P,
        clock_fn: C,
        fn_registry: Arc<dyn FunctionRegistry>,
    ) -> Result<Self, WasmFileError> {
        let wasm_path = wasm_path.as_ref();
        let mut linker = wasmtime::component::Linker::new(&engine);

        // Mock imported functions
        let wasm_component = WasmComponent::new(wasm_path, &engine)
            .map_err(|err| WasmFileError::DecodeError(wasm_path.to_owned(), err))?;
        for import in &wasm_component.exim.imports {
            if import.ifc_fqn.deref() == HOST_ACTIVITY_IFC_STRING {
                // Skip host-implemented functions
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
                            debug!("Skipping mocking of {ffqn}");
                        } else {
                            return Err(WasmFileError::LinkingError {
                                file: wasm_path.to_owned(),
                                reason: StrVariant::Arc(Arc::from(format!(
                                    "cannot add mock for imported function {ffqn}"
                                ))),
                                err: err.into(),
                            });
                        }
                    }
                }
            } else {
                trace!("Skipping interface {ifc_fqn}", ifc_fqn = import.ifc_fqn);
            }
        }
        WorkflowCtx::add_to_linker(&mut linker).map_err(|err| WasmFileError::LinkingError {
            file: wasm_path.to_owned(),
            reason: StrVariant::Arc(Arc::from("cannot add host activities".to_string())),
            err: err.into(),
        })?;
        Ok(Self {
            config,
            engine,
            linker,
            component: wasm_component.component,
            exim: wasm_component.exim,
            db_pool,
            clock_fn,
            fn_registry,
        })
    }
}

#[async_trait]
impl<C: ClockFn + 'static, DB: DbConnection + 'static, P: DbPool<DB> + 'static> Worker
    for WorkflowWorker<C, DB, P>
{
    fn exported_functions(&self) -> impl Iterator<Item = FunctionMetadata> {
        self.exim.exported_functions()
    }

    fn imported_functions(&self) -> impl Iterator<Item = FunctionMetadata> {
        self.exim.imported_functions()
    }

    #[allow(clippy::too_many_lines)]
    async fn run(&self, ctx: WorkerContext) -> WorkerResult {
        #[derive(thiserror::Error)]
        enum RunError<C: ClockFn + 'static, DB: DbConnection + 'static, P: DbPool<DB> + 'static> {
            #[error("worker error: `{0}`")]
            WorkerError(WorkerError, WorkflowCtx<C, DB, P>),
            #[error("wasm function call error: `{0}`")]
            FunctionCall(Box<dyn Error + Send + Sync>, WorkflowCtx<C, DB, P>),
        }
        trace!("Params: {params:?}", params = ctx.params);
        let timeout_error_container = Arc::new(std::sync::Mutex::new(WorkerResult::Err(
            WorkerError::IntermittentTimeout,
        )));
        let (instance, mut store) = {
            let seed = ctx.execution_id.random_part();
            let ctx = WorkflowCtx::new(
                ctx.execution_id,
                ctx.event_history,
                ctx.responses,
                seed,
                self.clock_fn.clone(),
                self.config.join_next_blocking_strategy,
                self.db_pool.clone(),
                ctx.version,
                ctx.execution_deadline,
                self.config.child_retry_exp_backoff,
                self.config.child_max_retries,
                self.config.non_blocking_event_batching,
                timeout_error_container.clone(),
                self.fn_registry.clone(),
            );
            let mut store = Store::new(&self.engine, ctx);
            let instance = match self
                .linker
                .instantiate_async(&mut store, &self.component)
                .await
            {
                Ok(instance) => instance,
                Err(err) => {
                    let reason = err.to_string();
                    let version = store.into_data().version;
                    if reason.starts_with("maximum concurrent") {
                        return WorkerResult::Err(WorkerError::LimitReached(reason, version));
                    }
                    return WorkerResult::Err(WorkerError::IntermittentError {
                        reason: StrVariant::Arc(Arc::from(format!("cannot instantiate - {err}"))),
                        err: Some(err.into()),
                        version,
                    });
                }
            };
            (instance, store)
        };
        store.epoch_deadline_callback(|_store_ctx| Ok(UpdateDeadline::Yield(1)));
        let call_function = async move {
            let func = {
                let mut exports = instance.exports(&mut store);
                let mut exports_instance = exports.root();
                let mut exports_instance = exports_instance
                    .instance(&ctx.ffqn.ifc_fqn)
                    .expect("interface must be found");
                exports_instance
                    .func(&ctx.ffqn.function_name)
                    .expect("function must be found")
            };
            let params = match ctx.params.as_vals(func.params(&store)) {
                Ok(params) => params,
                Err(err) => {
                    let workflow_ctx = store.into_data();
                    return Err(RunError::WorkerError(
                        WorkerError::FatalError(
                            FatalError::ParamsParsingError(err),
                            workflow_ctx.version.clone(),
                        ),
                        workflow_ctx,
                    ));
                }
            };
            let result_types = func.results(&mut store);
            let mut results = vec![Val::Bool(false); result_types.len()];
            if let Err(err) = func.call_async(&mut store, &params, &mut results).await {
                return Err(RunError::FunctionCall(err.into(), store.into_data()));
            } // guest panic exits here
            let result = match SupportedFunctionResult::new(
                results.into_iter().zip(result_types.iter().cloned()),
            ) {
                Ok(result) => result,
                Err(err) => {
                    let workflow_ctx = store.into_data();
                    return Err(RunError::WorkerError(
                        WorkerError::FatalError(
                            FatalError::ResultParsingError(err),
                            workflow_ctx.version.clone(),
                        ),
                        workflow_ctx,
                    ));
                }
            };
            if let Err(err) = func.post_return_async(&mut store).await {
                let workflow_ctx = store.into_data();
                return Err(RunError::WorkerError(
                    WorkerError::IntermittentError {
                        reason: StrVariant::Arc(Arc::from(format!(
                            "wasm post function call error - {err}"
                        ))),
                        err: Some(err.into()),
                        version: workflow_ctx.version.clone(),
                    },
                    workflow_ctx,
                ));
            }

            Ok((result, store.into_data()))
        };
        let started_at = (self.clock_fn)();
        let deadline_duration = (ctx.execution_deadline - started_at).to_std().unwrap();
        let stopwatch = now_tokio_instant(); // Not using `clock_fn` here is ok, value is only used for log reporting.
        tokio::select! { // future's liveness: Dropping the loser immediately.
            res = call_function => {
                match res {
                    Ok((supported_result, mut workflow_ctx)) => {
                        debug!(duration = ?stopwatch.elapsed(), ?deadline_duration, execution_deadline= %ctx.execution_deadline, "Finished");
                        if let Err(db_err) = workflow_ctx.flush().await {
                            WorkerResult::Err(WorkerError::DbError(db_err))
                        } else {
                            WorkerResult::Ok(supported_result, workflow_ctx.version)
                        }
                    },
                    Err(RunError::FunctionCall(err, mut workflow_ctx)) => {
                        if let Err(db_err) = workflow_ctx.flush().await {
                            return WorkerResult::Err(WorkerError::DbError(db_err));
                        }
                        let version = workflow_ctx.version;
                        if let Some(err) =  err
                            .source()
                            .and_then(|source| source.downcast_ref::<FunctionError>())
                        {
                            let worker_result = err.clone().into_worker_result(version);
                            if let WorkerResult::Err(err) = &worker_result {
                                info!(%err, duration = ?stopwatch.elapsed(), ?deadline_duration, execution_deadline = %ctx.execution_deadline, "Finished with a error");
                            } else if matches!(worker_result, WorkerResult::ChildExecutionRequest | WorkerResult::DelayRequest) {
                                info!(duration = ?stopwatch.elapsed(), ?deadline_duration, execution_deadline = %ctx.execution_deadline, "Interrupt requested");
                            } else {
                                info!(duration = ?stopwatch.elapsed(), ?deadline_duration, execution_deadline = %ctx.execution_deadline, "Finished successfuly");
                            }
                            worker_result
                        } else  {
                            let err = WorkerError::IntermittentError {
                                reason: StrVariant::Arc(Arc::from(format!("uncategorized function call error - {err}"))),
                                err: Some(err),
                                version,
                            };
                            info!(%err, duration = ?stopwatch.elapsed(), ?deadline_duration, execution_deadline = %ctx.execution_deadline, "Finished with an error");
                            WorkerResult::Err(err)
                        }
                    }
                    Err(RunError::WorkerError(err, mut workflow_ctx)) => {
                        if let Err(db_err) = workflow_ctx.flush().await {
                            WorkerResult::Err(WorkerError::DbError(db_err))
                        } else {
                            WorkerResult::Err(err)
                        }
                    },
                }
            },
            () = tokio::time::sleep(deadline_duration) => {
                // not flushing the workflow_ctx as it would introduce locking.
                let worker_result = std::mem::replace(&mut *timeout_error_container.lock().unwrap(), WorkerResult::Err(WorkerError::IntermittentTimeout));
                info!(duration = ?stopwatch.elapsed(), ?deadline_duration, execution_deadline = %ctx.execution_deadline, now = %(self.clock_fn)(), "Timing out with {worker_result:?}");
                worker_result
            }
        }
    }
}

#[cfg(test)]
mod tests {
    // TODO: test timeouts, retries
    use super::*;
    use crate::{
        activity_worker::tests::{spawn_activity_fibo, FIBO_10_INPUT, FIBO_10_OUTPUT},
        engines::{EngineConfig, Engines},
        tests::{fn_registry_dummy, fn_registry_parsing_wasm},
    };
    use assert_matches::assert_matches;
    use concepts::{
        storage::{wait_for_pending_state_fn, CreateRequest, DbConnection, PendingState},
        FinishedExecutionError,
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

    fn spawn_workflow<DB: DbConnection + 'static, P: DbPool<DB> + 'static>(
        db_pool: P,
        wasm_path: &'static str,
        clock_fn: impl ClockFn + 'static,
        join_next_blocking_strategy: JoinNextBlockingStrategy,
        non_blocking_event_batching: NonBlockingEventBatching,
        fn_registry: Arc<dyn FunctionRegistry>,
    ) -> ExecutorTaskHandle {
        let workflow_engine = Engines::get_workflow_engine(EngineConfig::on_demand()).unwrap();
        let worker = Arc::new(
            WorkflowWorker::new_with_config(
                wasm_path,
                WorkflowConfig {
                    config_id: ComponentConfigHash::dummy(),
                    join_next_blocking_strategy,
                    child_retry_exp_backoff: Duration::ZERO,
                    child_max_retries: 0,
                    non_blocking_event_batching,
                },
                workflow_engine,
                db_pool.clone(),
                clock_fn.clone(),
                fn_registry,
            )
            .unwrap(),
        );

        let exec_config = ExecConfig {
            batch_size: 1,
            lock_expiry: Duration::from_secs(3),
            tick_sleep: TICK_SLEEP,
            config_id: ComponentConfigHash::dummy(),
        };
        ExecTask::spawn_new(worker, exec_config, clock_fn, db_pool, None)
    }

    pub(crate) fn spawn_workflow_fibo<DB: DbConnection + 'static, P: DbPool<DB> + 'static>(
        db_pool: P,
        clock_fn: impl ClockFn + 'static,
        join_next_blocking_strategy: JoinNextBlockingStrategy,
        non_blocking_event_batching: NonBlockingEventBatching,
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
    }

    #[rstest]
    #[tokio::test]
    async fn fibo_workflow_should_schedule_fibo_activity_mem(
        #[values(JoinNextBlockingStrategy::Interrupt, JoinNextBlockingStrategy::Await)]
        join_next_blocking_strategy: JoinNextBlockingStrategy,
        #[values(NonBlockingEventBatching::Disabled, NonBlockingEventBatching::Enabled)]
        batching: NonBlockingEventBatching,
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
        #[values(NonBlockingEventBatching::Disabled, NonBlockingEventBatching::Enabled)]
        batching: NonBlockingEventBatching,
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
        batching: NonBlockingEventBatching,
    ) {
        const INPUT_ITERATIONS: u32 = 1;
        let _guard = test_utils::set_up();
        let fn_registry = fn_registry_parsing_wasm(&[
            test_programs_fibo_workflow_builder::TEST_PROGRAMS_FIBO_WORKFLOW,
            test_programs_fibo_activity_builder::TEST_PROGRAMS_FIBO_ACTIVITY,
        ])
        .await;
        let workflow_exec_task = spawn_workflow_fibo(
            db_pool.clone(),
            sim_clock.get_clock_fn(),
            join_next_blocking_strategy,
            batching,
            fn_registry,
        );
        // Create an execution.
        let execution_id = ExecutionId::from_parts(0, 0);
        let created_at = sim_clock.now();
        let db_connection = db_pool.connection();

        let params = Params::from_json_array(json!([FIBO_10_INPUT, INPUT_ITERATIONS])).unwrap();
        db_connection
            .create(CreateRequest {
                created_at,
                execution_id,
                ffqn: FIBO_WORKFLOW_FFQN,
                params,
                parent: None,
                scheduled_at: created_at,
                retry_exp_backoff: Duration::ZERO,
                max_retries: 0,
                config_id: ComponentConfigHash::dummy(),
                return_type: None,
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
        let activity_exec_task = spawn_activity_fibo(db_pool.clone(), sim_clock.get_clock_fn());

        let res = db_connection
            .wait_for_finished_result(execution_id, None)
            .await
            .unwrap()
            .unwrap();
        let res = assert_matches!(res, SupportedFunctionResult::Infallible(val) => val);
        assert_eq!(
            FIBO_10_OUTPUT,
            assert_matches!(res, WastValWithType{ value: WastVal::U64(actual), r#type: TypeWrapper::U64} => actual),
        );
        workflow_exec_task.close().await;
        activity_exec_task.close().await;
    }

    #[rstest]
    #[tokio::test]
    async fn fibo_workflow_with_no_activity_should_fail(
        #[values(JoinNextBlockingStrategy::Interrupt, JoinNextBlockingStrategy::Await)]
        join_next_blocking_strategy: JoinNextBlockingStrategy,
        #[values(NonBlockingEventBatching::Disabled, NonBlockingEventBatching::Enabled)]
        batching: NonBlockingEventBatching,
    ) {
        const INPUT_ITERATIONS: u32 = 1;
        let sim_clock = SimClock::default();
        let (_guard, db_pool) = Database::Memory.set_up().await;
        let _guard = test_utils::set_up();
        let fn_registry = fn_registry_parsing_wasm(&[
            test_programs_fibo_workflow_builder::TEST_PROGRAMS_FIBO_WORKFLOW,
        ])
        .await;
        let workflow_exec_task = spawn_workflow_fibo(
            db_pool.clone(),
            sim_clock.get_clock_fn(),
            join_next_blocking_strategy,
            batching,
            fn_registry,
        );
        // Create an execution.
        let execution_id = ExecutionId::from_parts(0, 0);
        let created_at = sim_clock.now();
        let db_connection = db_pool.connection();

        let params = Params::from_json_array(json!([FIBO_10_INPUT, INPUT_ITERATIONS])).unwrap();
        db_connection
            .create(CreateRequest {
                created_at,
                execution_id,
                ffqn: FIBO_WORKFLOW_FFQN,
                params,
                parent: None,
                scheduled_at: created_at,
                retry_exp_backoff: Duration::ZERO,
                max_retries: 0,
                config_id: ComponentConfigHash::dummy(),
                return_type: None,
            })
            .await
            .unwrap();
        info!("Should end as Failed");
        let finished_result = wait_for_pending_state_fn(
            &db_connection,
            execution_id,
            |exe_history| exe_history.finished_result().cloned(),
            None,
        )
        .await
        .unwrap();
        let finished_result = finished_result.unwrap_err();
        let finished_result = assert_matches!(
            finished_result,
            FinishedExecutionError::PermanentFailure( reason) => reason.to_string()
        );
        assert_eq!(
            "attempted to submit an execution with no active component, \
                function metadata not found for testing:fibo/fibo.fibo",
            finished_result
        );

        workflow_exec_task.close().await;
        drop(db_connection);
        db_pool.close().await.unwrap();
    }

    fn get_workflow_worker<C: ClockFn, DB: DbConnection + 'static, P: DbPool<DB> + 'static>(
        db_pool: P,
        clock_fn: C,
        join_next_blocking_strategy: JoinNextBlockingStrategy,
        non_blocking_event_batching: NonBlockingEventBatching,
        fn_registry: Arc<dyn FunctionRegistry>,
    ) -> Arc<WorkflowWorker<C, DB, P>> {
        Arc::new(
            WorkflowWorker::new_with_config(
                test_programs_sleep_workflow_builder::TEST_PROGRAMS_SLEEP_WORKFLOW,
                WorkflowConfig {
                    config_id: ComponentConfigHash::dummy(),
                    join_next_blocking_strategy,
                    child_retry_exp_backoff: Duration::ZERO,
                    child_max_retries: 0,
                    non_blocking_event_batching,
                },
                Engines::get_workflow_engine(EngineConfig::on_demand()).unwrap(),
                db_pool,
                clock_fn,
                fn_registry,
            )
            .unwrap(),
        )
    }

    pub(crate) fn spawn_workflow_sleep<DB: DbConnection + 'static, P: DbPool<DB> + 'static>(
        db_pool: P,
        clock_fn: impl ClockFn + 'static,
        join_next_blocking_strategy: JoinNextBlockingStrategy,
        non_blocking_event_batching: NonBlockingEventBatching,
        fn_registry: Arc<dyn FunctionRegistry>,
    ) -> ExecutorTaskHandle {
        let worker = get_workflow_worker(
            db_pool.clone(),
            clock_fn.clone(),
            join_next_blocking_strategy,
            non_blocking_event_batching,
            fn_registry,
        );
        let exec_config = ExecConfig {
            batch_size: 1,
            lock_expiry: Duration::from_secs(1),
            tick_sleep: TICK_SLEEP,
            config_id: ComponentConfigHash::dummy(),
        };
        ExecTask::spawn_new(worker, exec_config, clock_fn, db_pool, None)
    }

    #[rstest]
    #[tokio::test]
    async fn sleep_should_be_persisted_after_executor_restart(
        #[values(JoinNextBlockingStrategy::Interrupt, JoinNextBlockingStrategy::Await)]
        join_next_blocking_strategy: JoinNextBlockingStrategy,
        #[values(NonBlockingEventBatching::Disabled, NonBlockingEventBatching::Enabled)]
        batching: NonBlockingEventBatching,
    ) {
        const SLEEP_MILLIS: u32 = 100;
        let _guard = test_utils::set_up();
        let sim_clock = SimClock::default();
        let (_guard, db_pool) = Database::Memory.set_up().await;

        let empty_fn_registry = fn_registry_dummy(&[]);

        let workflow_exec_task = spawn_workflow_sleep(
            db_pool.clone(),
            sim_clock.get_clock_fn(),
            join_next_blocking_strategy,
            batching,
            empty_fn_registry.clone(),
        );
        let timers_watcher_task = expired_timers_watcher::TimersWatcherTask::spawn_new(
            db_pool.connection(),
            expired_timers_watcher::TimersWatcherConfig {
                tick_sleep: TICK_SLEEP,
                clock_fn: sim_clock.get_clock_fn(),
            },
        );
        let execution_id = ExecutionId::generate();
        let db_connection = db_pool.connection();
        let params = Params::from_json_array(json!([SLEEP_MILLIS])).unwrap();
        db_connection
            .create(CreateRequest {
                created_at: sim_clock.now(),
                execution_id,
                ffqn: SLEEP_HOST_ACTIVITY_FFQN,
                params,
                parent: None,
                scheduled_at: sim_clock.now(),
                retry_exp_backoff: Duration::ZERO,
                max_retries: 0,
                config_id: ComponentConfigHash::dummy(),
                return_type: None,
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
            sim_clock.get_clock_fn(),
            join_next_blocking_strategy,
            batching,
            empty_fn_registry,
        );
        let res = db_connection
            .wait_for_finished_result(execution_id, None)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(SupportedFunctionResult::None, res);
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
        #[values(NonBlockingEventBatching::Disabled, NonBlockingEventBatching::Enabled)]
        batching: NonBlockingEventBatching,
    ) {
        use crate::activity_worker::tests::spawn_activity;
        use chrono::DateTime;
        use std::ops::Deref;
        use wiremock::{
            matchers::{method, path},
            Mock, MockServer, ResponseTemplate,
        };

        const BODY: &str = "ok";
        pub const HTTP_GET_WORKFLOW_FFQN: FunctionFqn =
            FunctionFqn::new_static_tuple(test_programs_http_get_workflow_builder::exports::testing::http_workflow::workflow::GET_SUCCESSFUL);

        test_utils::set_up();
        let sim_clock = SimClock::new(DateTime::default());
        let (_guard, db_pool) = Database::Memory.set_up().await;
        let created_at = sim_clock.now();
        let db_connection = db_pool.connection();
        let fn_registry = fn_registry_parsing_wasm(&[
            test_programs_http_get_activity_builder::TEST_PROGRAMS_HTTP_GET_ACTIVITY,
            test_programs_http_get_workflow_builder::TEST_PROGRAMS_HTTP_GET_WORKFLOW,
        ])
        .await;
        let activity_exec_task = spawn_activity(
            db_pool.clone(),
            test_programs_http_get_activity_builder::TEST_PROGRAMS_HTTP_GET_ACTIVITY,
            sim_clock.get_clock_fn(),
        );
        let workflow_exec_task = spawn_workflow(
            db_pool.clone(),
            test_programs_http_get_workflow_builder::TEST_PROGRAMS_HTTP_GET_WORKFLOW,
            sim_clock.get_clock_fn(),
            join_next_blocking_strategy,
            batching,
            fn_registry,
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
        let params = Params::from_json_array(json!([url])).unwrap();
        // Create an execution.
        let execution_id = ExecutionId::generate();
        db_connection
            .create(CreateRequest {
                created_at,
                execution_id,
                ffqn: HTTP_GET_WORKFLOW_FFQN,
                params,
                parent: None,
                scheduled_at: created_at,
                retry_exp_backoff: Duration::ZERO,
                max_retries: 0,
                config_id: ComponentConfigHash::dummy(),
                return_type: None,
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
        #[values(NonBlockingEventBatching::Disabled, NonBlockingEventBatching::Enabled)]
        batching: NonBlockingEventBatching,
    ) {
        use crate::activity_worker::tests::spawn_activity;
        use chrono::DateTime;
        use std::ops::Deref;
        use wiremock::{
            matchers::{method, path},
            Mock, MockServer, ResponseTemplate,
        };
        const BODY: &str = "ok";
        const HTTP_GET_WORKFLOW_FFQN: FunctionFqn =
            FunctionFqn::new_static_tuple(test_programs_http_get_workflow_builder::exports::testing::http_workflow::workflow::GET_SUCCESSFUL_CONCURRENTLY_STRESS);

        let concurrency: u32 = std::env::var("CONCURRENCY")
            .map(|c| c.parse::<u32>())
            .ok()
            .transpose()
            .ok()
            .flatten()
            .unwrap_or(5);

        test_utils::set_up();
        let sim_clock = SimClock::new(DateTime::default());
        let created_at = sim_clock.now();
        let (_guard, db_pool) = db.set_up().await;
        let db_connection = db_pool.connection();
        let fn_registry = fn_registry_parsing_wasm(&[
            test_programs_http_get_activity_builder::TEST_PROGRAMS_HTTP_GET_ACTIVITY,
            test_programs_http_get_workflow_builder::TEST_PROGRAMS_HTTP_GET_WORKFLOW,
        ])
        .await;

        let activity_exec_task = spawn_activity(
            db_pool.clone(),
            test_programs_http_get_activity_builder::TEST_PROGRAMS_HTTP_GET_ACTIVITY,
            sim_clock.get_clock_fn(),
        );
        let workflow_exec_task = spawn_workflow(
            db_pool.clone(),
            test_programs_http_get_workflow_builder::TEST_PROGRAMS_HTTP_GET_WORKFLOW,
            sim_clock.get_clock_fn(),
            join_next_strategy,
            batching,
            fn_registry,
        );
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/"))
            .respond_with(ResponseTemplate::new(200).set_body_string(BODY))
            .mount(&server)
            .await;
        debug!("started mock server on {}", server.address());
        let url = format!("http://127.0.0.1:{}/", server.address().port());
        let params = Params::from_json_array(json!([url, concurrency])).unwrap();
        // Create an execution.
        let execution_id = ExecutionId::generate();

        db_connection
            .create(CreateRequest {
                created_at,
                execution_id,
                ffqn: HTTP_GET_WORKFLOW_FFQN,
                params,
                parent: None,
                scheduled_at: created_at,
                retry_exp_backoff: Duration::from_millis(0),
                max_retries: concurrency - 1, // response can conflict with next ChildExecutionRequest
                config_id: ComponentConfigHash::dummy(),
                return_type: None,
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
        #[values(NonBlockingEventBatching::Disabled, NonBlockingEventBatching::Enabled)]
        batching: NonBlockingEventBatching,
    ) {
        use concepts::prefixed_ulid::ExecutorId;

        const SLEEP_MILLIS: u32 = 100;
        const RESCHEDULE_FFQN: FunctionFqn =
            FunctionFqn::new_static_tuple(test_programs_sleep_workflow_builder::exports::testing::sleep_workflow::workflow::RESCHEDULE);
        let _guard = test_utils::set_up();
        let sim_clock = SimClock::default();
        let (_guard, db_pool) = db.set_up().await;
        let fn_registry = fn_registry_dummy(&[RESCHEDULE_FFQN]);
        let worker = get_workflow_worker(
            db_pool.clone(),
            sim_clock.get_clock_fn(),
            join_next_strategy,
            batching,
            fn_registry,
        );
        let execution_id = ExecutionId::generate();
        let db_connection = db_pool.connection();

        let params = Params::from_json_array(json!([SLEEP_MILLIS])).unwrap();
        db_connection
            .create(CreateRequest {
                created_at: sim_clock.now(),
                execution_id,
                ffqn: RESCHEDULE_FFQN,
                params: params.clone(),
                parent: None,
                scheduled_at: sim_clock.now(),
                retry_exp_backoff: Duration::ZERO,
                max_retries: 0,
                config_id: ComponentConfigHash::dummy(),
                return_type: None,
            })
            .await
            .unwrap();
        // tick2 + await should mark the execution finished.
        let exec_task = ExecTask::new(
            worker,
            ExecConfig {
                batch_size: 1,
                lock_expiry: Duration::from_secs(1),
                tick_sleep: TICK_SLEEP,
                config_id: ComponentConfigHash::dummy(),
            },
            sim_clock.get_clock_fn(),
            db_pool.clone(),
            Arc::new([RESCHEDULE_FFQN]),
            None,
        );
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
            res.finished_result().unwrap(),
            Ok(SupportedFunctionResult::None)
        );
        // New execution should be pending in SLEEP_MILLIS.
        sim_clock
            .move_time_forward(Duration::from_millis(u64::from(SLEEP_MILLIS)))
            .await;
        let mut next_pending = db_pool
            .connection()
            .lock_pending(
                10,
                sim_clock.now(),
                Arc::from([RESCHEDULE_FFQN]),
                sim_clock.now(),
                ExecutorId::generate(),
                sim_clock.now() + Duration::from_secs(1),
            )
            .await
            .unwrap();
        assert_eq!(1, next_pending.len());
        let next_pending = next_pending.pop().unwrap();
        assert!(next_pending.parent.is_none());
        assert_eq!(params, next_pending.params);
        drop(exec_task);
        db_pool.close().await.unwrap();
    }
}
