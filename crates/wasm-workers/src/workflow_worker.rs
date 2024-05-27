use crate::workflow_ctx::{FunctionError, WorkflowCtx};
use crate::{EngineConfig, WasmComponent, WasmFileError};
use async_trait::async_trait;
use concepts::prefixed_ulid::ConfigId;
use concepts::storage::{DbConnection, DbPool, Version};
use concepts::SupportedFunctionResult;
use concepts::{FunctionFqn, StrVariant};
use derivative::Derivative;
use executor::worker::{FatalError, WorkerContext, WorkerResult};
use executor::worker::{Worker, WorkerError};
use std::error::Error;
use std::marker::PhantomData;
use std::ops::Deref;
use std::time::Duration;
use std::{fmt::Debug, sync::Arc};
use tracing::{debug, info, trace};
use utils::time::{now_tokio_instant, ClockFn};
use utils::wasm_tools::ExIm;
use wasmtime::{component::Val, Engine};
use wasmtime::{Store, UpdateDeadline};

#[must_use]
pub fn workflow_engine(config: EngineConfig) -> Arc<Engine> {
    let mut wasmtime_config = wasmtime::Config::new();
    wasmtime_config.wasm_backtrace(true);
    wasmtime_config.wasm_backtrace_details(wasmtime::WasmBacktraceDetails::Disable);
    wasmtime_config.wasm_component_model(true);
    wasmtime_config.async_support(true);
    wasmtime_config.allocation_strategy(config.allocation_strategy);
    wasmtime_config.epoch_interruption(true);
    Arc::new(Engine::new(&wasmtime_config).unwrap())
}

/// Defines behavior of the wasm runtime when `HistoryEvent::JoinNextBlocking` is requested.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub enum JoinNextBlockingStrategy {
    /// Shut down the current runtime. When the `JoinSetResponse` is appended, workflow is reexecuted with a new `RunId`.
    #[default]
    Interrupt,
    /// Keep the execution hot. Worker will poll the database until the execution lock expires.
    Await,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum NonBlockingEventBatching {
    #[default]
    Disabled,
    Enabled,
}

#[derive(Clone, Derivative)]
#[derivative(Debug)]
pub struct WorkflowConfig<C: ClockFn, DB: DbConnection, P: DbPool<DB>> {
    pub config_id: ConfigId,
    pub clock_fn: C,
    pub join_next_blocking_strategy: JoinNextBlockingStrategy,
    #[derivative(Debug = "ignore")]
    pub db_pool: P,
    pub child_retry_exp_backoff: Duration,
    pub child_max_retries: u32,
    pub non_blocking_event_batching: NonBlockingEventBatching,
    #[derivative(Debug = "ignore")]
    pub phantom_data: PhantomData<DB>,
}

#[derive(Clone)]
pub struct WorkflowWorker<C: ClockFn, DB: DbConnection, P: DbPool<DB>> {
    config: WorkflowConfig<C, DB, P>,
    engine: Arc<Engine>,
    linker: wasmtime::component::Linker<WorkflowCtx<C, DB, P>>,
    component: wasmtime::component::Component,
    exim: ExIm,
}

impl<C: ClockFn, DB: DbConnection, P: DbPool<DB>> WorkflowWorker<C, DB, P> {
    #[tracing::instrument(skip_all, fields(wasm_path = %wasm_component.wasm_path, config_id = %config.config_id))]
    pub fn new_with_config(
        wasm_component: WasmComponent,
        config: WorkflowConfig<C, DB, P>,
        engine: Arc<Engine>,
    ) -> Result<Self, WasmFileError> {
        const HOST_ACTIVITY_IFC_STRING: &str = "my-org:workflow-engine/host-activities";
        let mut linker = wasmtime::component::Linker::new(&engine);

        // Mock imported functions
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
                                file: wasm_component.wasm_path.clone(),
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
            file: wasm_component.wasm_path.clone(),
            reason: StrVariant::Arc(Arc::from("cannot add host activities".to_string())),
            err: err.into(),
        })?;
        Ok(Self {
            config,
            engine,
            linker,
            component: wasm_component.component,
            exim: wasm_component.exim,
        })
    }
}

#[async_trait]
impl<C: ClockFn + 'static, DB: DbConnection + 'static, P: DbPool<DB> + 'static> Worker
    for WorkflowWorker<C, DB, P>
{
    fn exported_functions(&self) -> impl Iterator<Item = &FunctionFqn> {
        self.exim.exported_functions()
    }

    #[allow(clippy::too_many_lines)]
    async fn run(&self, ctx: WorkerContext) -> WorkerResult {
        #[derive(Debug, thiserror::Error)]
        enum RunError {
            #[error(transparent)]
            WorkerError(#[from] WorkerError),
            #[error("wasm function call error: `{0}`")]
            FunctionCall(Box<dyn Error + Send + Sync>, Version),
        }
        trace!("Params: {params:?}", params = ctx.params);
        let timeout_error = Arc::new(std::sync::Mutex::new(WorkerResult::Err(
            WorkerError::IntermittentTimeout,
        )));
        let (instance, mut store) = {
            let seed = ctx.execution_id.random_part();
            let ctx = WorkflowCtx::new(
                //TODO: merge WorkerContext into
                ctx.execution_id,
                ctx.event_history,
                ctx.responses,
                seed,
                self.config.clock_fn.clone(),
                self.config.join_next_blocking_strategy,
                self.config.db_pool.clone(),
                ctx.version,
                ctx.execution_deadline,
                self.config.child_retry_exp_backoff,
                self.config.child_max_retries,
                self.config.non_blocking_event_batching,
                timeout_error.clone(),
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
                    return Err(RunError::WorkerError(WorkerError::FatalError(
                        FatalError::ParamsParsingError(err),
                        store.into_data().version,
                    )));
                }
            };
            let result_types = func.results(&mut store);
            let mut results = vec![Val::Bool(false); result_types.len()];
            if let Err(err) = func.call_async(&mut store, &params, &mut results).await {
                return Err(RunError::FunctionCall(
                    err.into(),
                    store.into_data().version,
                ));
            } // guest panic exits here
            let result = match SupportedFunctionResult::new(
                results.into_iter().zip(result_types.iter().cloned()),
            ) {
                Ok(result) => result,
                Err(err) => {
                    return Err(RunError::WorkerError(WorkerError::FatalError(
                        FatalError::ResultParsingError(err),
                        store.into_data().version,
                    )));
                }
            };
            if let Err(err) = func.post_return_async(&mut store).await {
                return Err(RunError::WorkerError(WorkerError::IntermittentError {
                    reason: StrVariant::Arc(Arc::from(format!(
                        "wasm post function call error - {err}"
                    ))),
                    err: Some(err.into()),
                    version: store.into_data().version,
                }));
            }

            Ok((result, store.into_data().version))
        };
        let started_at = (self.config.clock_fn)();
        let deadline_duration = (ctx.execution_deadline - started_at).to_std().unwrap();
        let stopwatch = now_tokio_instant(); // Not using `clock_fn` here is ok, value is only used for log reporting.
        tokio::select! {
            res = call_function => {
                match res {
                    Ok((supported_result, version)) => {
                        debug!(duration = ?stopwatch.elapsed(), ?deadline_duration, execution_deadline= %ctx.execution_deadline, "Finished");
                        WorkerResult::Ok(supported_result, version)
                    },
                    Err(RunError::FunctionCall(err, version)) => {
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
                    Err(RunError::WorkerError(err)) => WorkerResult::Err(err),
                }
            },
            () = tokio::time::sleep(deadline_duration) => {
                let worker_result = std::mem::replace(&mut *timeout_error.lock().unwrap(), WorkerResult::Err(WorkerError::IntermittentTimeout));
                info!(duration = ?stopwatch.elapsed(), ?deadline_duration, execution_deadline = %ctx.execution_deadline, now = %(self.config.clock_fn)(), "Timing out with {worker_result:?}");
                worker_result
            }
        }
    }
}

mod valuable {
    use super::WorkflowWorker;
    use concepts::storage::{DbConnection, DbPool};
    use utils::time::ClockFn;

    static FIELDS: &[::valuable::NamedField<'static>] = &[::valuable::NamedField::new("config_id")];
    impl<C: ClockFn, DB: DbConnection, P: DbPool<DB>> ::valuable::Structable
        for WorkflowWorker<C, DB, P>
    {
        fn definition(&self) -> ::valuable::StructDef<'_> {
            ::valuable::StructDef::new_static("WorkflowWorker", ::valuable::Fields::Named(FIELDS))
        }
    }
    impl<C: ClockFn, DB: DbConnection, P: DbPool<DB>> ::valuable::Valuable
        for WorkflowWorker<C, DB, P>
    {
        fn as_value(&self) -> ::valuable::Value<'_> {
            ::valuable::Value::Structable(self)
        }
        fn visit(&self, visitor: &mut dyn ::valuable::Visit) {
            visitor.visit_named_fields(&::valuable::NamedValues::new(
                FIELDS,
                &[::valuable::Value::String(
                    &self.config.config_id.to_string(),
                )],
            ));
        }
    }
}

#[cfg(test)]
mod tests {
    // TODO: test timeouts, retries
    use super::*;
    use crate::{
        activity_worker::tests::{spawn_activity_fibo, FIBO_10_INPUT, FIBO_10_OUTPUT},
        EngineConfig,
    };
    use assert_matches::assert_matches;
    use concepts::storage::{wait_for_pending_state_fn, CreateRequest, DbConnection, PendingState};
    use concepts::{prefixed_ulid::ConfigId, ExecutionId, Params};
    use db_tests::Database;
    use executor::{
        executor::{ExecConfig, ExecTask, ExecutorTaskHandle},
        expired_timers_watcher,
    };
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
        ffqn: FunctionFqn,
        clock_fn: impl ClockFn + 'static,
        join_next_blocking_strategy: JoinNextBlockingStrategy,
    ) -> ExecutorTaskHandle {
        let workflow_engine = workflow_engine(EngineConfig::default());
        let worker = Arc::new(
            WorkflowWorker::new_with_config(
                WasmComponent::new(StrVariant::Static(wasm_path), &workflow_engine).unwrap(),
                WorkflowConfig {
                    db_pool: db_pool.clone(),
                    config_id: ConfigId::generate(),
                    clock_fn: clock_fn.clone(),
                    join_next_blocking_strategy,
                    child_retry_exp_backoff: Duration::ZERO,
                    child_max_retries: 0,
                    non_blocking_event_batching: NonBlockingEventBatching::default(),
                    phantom_data: PhantomData,
                },
                workflow_engine,
            )
            .unwrap(),
        );

        let exec_config = ExecConfig {
            ffqns: vec![ffqn],
            batch_size: 1,
            lock_expiry: Duration::from_secs(1),
            tick_sleep: TICK_SLEEP,
            clock_fn,
        };
        ExecTask::spawn_new(worker, exec_config, db_pool, None)
    }

    pub(crate) fn spawn_workflow_fibo<DB: DbConnection + 'static, P: DbPool<DB> + 'static>(
        db_pool: P,
        clock_fn: impl ClockFn + 'static,
    ) -> ExecutorTaskHandle {
        spawn_workflow(
            db_pool,
            test_programs_fibo_workflow_builder::TEST_PROGRAMS_FIBO_WORKFLOW,
            FIBO_WORKFLOW_FFQN,
            clock_fn,
            JoinNextBlockingStrategy::default(),
        )
    }

    #[tokio::test]
    async fn fibo_workflow_should_schedule_fibo_activity_mem() {
        let sim_clock = SimClock::default();
        let (_guard, db_pool) = Database::Memory.set_up().await;
        fibo_workflow_should_schedule_fibo_activity(db_pool.clone(), sim_clock).await;
        db_pool.close().await.unwrap();
    }

    #[cfg(not(madsim))]
    #[tokio::test]
    async fn fibo_workflow_should_schedule_fibo_activity_sqlite() {
        let sim_clock = SimClock::default();
        let (_guard, db_pool) = Database::Sqlite.set_up().await;
        fibo_workflow_should_schedule_fibo_activity(db_pool.clone(), sim_clock).await;
        db_pool.close().await.unwrap();
    }

    async fn fibo_workflow_should_schedule_fibo_activity<
        DB: DbConnection + 'static,
        P: DbPool<DB> + 'static,
    >(
        db_pool: P,
        sim_clock: SimClock,
    ) {
        const INPUT_ITERATIONS: u32 = 1;
        let _guard = test_utils::set_up();
        let workflow_exec_task = spawn_workflow_fibo(db_pool.clone(), sim_clock.get_clock_fn());
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

    pub(crate) fn spawn_workflow_sleep<DB: DbConnection + 'static, P: DbPool<DB> + 'static>(
        db_pool: P,
        clock_fn: impl ClockFn + 'static,
    ) -> ExecutorTaskHandle {
        let workflow_engine = workflow_engine(EngineConfig::default());
        let worker = Arc::new(
            WorkflowWorker::new_with_config(
                WasmComponent::new(
                    StrVariant::Static(
                        test_programs_sleep_workflow_builder::TEST_PROGRAMS_SLEEP_WORKFLOW,
                    ),
                    &workflow_engine,
                )
                .unwrap(),
                WorkflowConfig {
                    db_pool: db_pool.clone(),
                    config_id: ConfigId::generate(),
                    clock_fn: clock_fn.clone(),
                    join_next_blocking_strategy: JoinNextBlockingStrategy::default(),
                    child_retry_exp_backoff: Duration::ZERO,
                    child_max_retries: 0,
                    non_blocking_event_batching: NonBlockingEventBatching::default(),
                    phantom_data: PhantomData,
                },
                workflow_engine,
            )
            .unwrap(),
        );

        let exec_config = ExecConfig {
            ffqns: vec![SLEEP_HOST_ACTIVITY_FFQN],
            batch_size: 1,
            lock_expiry: Duration::from_secs(1),
            tick_sleep: TICK_SLEEP,
            clock_fn,
        };
        ExecTask::spawn_new(worker, exec_config, db_pool, None)
    }

    // TODO: test with JoinNextBlockingStrategy::Await
    #[tokio::test]
    async fn sleep_should_be_persisted_after_executor_restart() {
        const SLEEP_MILLIS: u32 = 100;
        let _guard = test_utils::set_up();
        let sim_clock = SimClock::default();
        let (_guard, db_pool) = Database::Memory.set_up().await;

        let workflow_exec_task = spawn_workflow_sleep(db_pool.clone(), sim_clock.get_clock_fn());
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
        let workflow_exec_task = spawn_workflow_sleep(db_pool.clone(), sim_clock.get_clock_fn());
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
    #[tokio::test]
    async fn http_get() {
        use crate::activity_worker::tests::{
            spawn_activity, wasmtime_nosim::HTTP_GET_SUCCESSFUL_ACTIVITY,
        };
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

        let activity_exec_task = spawn_activity(
            db_pool.clone(),
            test_programs_http_get_activity_builder::TEST_PROGRAMS_HTTP_GET_ACTIVITY,
            HTTP_GET_SUCCESSFUL_ACTIVITY,
            sim_clock.get_clock_fn(),
        );
        let workflow_exec_task = spawn_workflow(
            db_pool.clone(),
            test_programs_http_get_workflow_builder::TEST_PROGRAMS_HTTP_GET_WORKFLOW,
            HTTP_GET_WORKFLOW_FFQN,
            sim_clock.get_clock_fn(),
            JoinNextBlockingStrategy::default(),
        );
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/"))
            .respond_with(ResponseTemplate::new(200).set_body_string(BODY))
            .expect(1)
            .mount(&server)
            .await;
        debug!("started mock server on {}", server.address());
        let authority = format!("127.0.0.1:{}", server.address().port());
        let params = Params::from_json_array(json!([authority, "/"])).unwrap();
        // Create an execution.
        let execution_id = ExecutionId::generate();
        let created_at = sim_clock.now();
        let db_connection = db_pool.connection();
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
    ) {
        use crate::activity_worker::tests::{
            spawn_activity, wasmtime_nosim::HTTP_GET_SUCCESSFUL_ACTIVITY,
        };
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
        let (_guard, db_pool) = db.set_up().await;
        let activity_exec_task = spawn_activity(
            db_pool.clone(),
            test_programs_http_get_activity_builder::TEST_PROGRAMS_HTTP_GET_ACTIVITY,
            HTTP_GET_SUCCESSFUL_ACTIVITY,
            sim_clock.get_clock_fn(),
        );
        let workflow_exec_task = spawn_workflow(
            db_pool.clone(),
            test_programs_http_get_workflow_builder::TEST_PROGRAMS_HTTP_GET_WORKFLOW,
            HTTP_GET_WORKFLOW_FFQN,
            sim_clock.get_clock_fn(),
            join_next_strategy,
        );
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/"))
            .respond_with(ResponseTemplate::new(200).set_body_string(BODY))
            .mount(&server)
            .await;
        debug!("started mock server on {}", server.address());
        let authority = format!("127.0.0.1:{}", server.address().port());
        let params = Params::from_json_array(json!([authority, "/", concurrency])).unwrap();
        // Create an execution.
        let execution_id = ExecutionId::generate();
        let created_at = sim_clock.now();
        let db_connection = db_pool.connection();
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
}
