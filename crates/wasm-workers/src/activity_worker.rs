use crate::activity_ctx::ActivityCtx;
use crate::envvar::EnvVar;
use crate::std_output_stream::StdOutput;
use crate::workflow_ctx::log_activities;
use crate::WasmFileError;
use async_trait::async_trait;
use concepts::{ConfigId, FunctionFqn, SupportedFunctionReturnValue};
use concepts::{FunctionMetadata, StrVariant};
use executor::worker::{FatalError, WorkerContext, WorkerResult};
use executor::worker::{Worker, WorkerError};
use std::path::Path;
use std::time::Duration;
use std::{fmt::Debug, sync::Arc};
use tracing::{error, info, trace};
use utils::time::{now_tokio_instant, ClockFn};
use utils::wasm_tools::{ExIm, WasmComponent};
use wasmtime::component::{ComponentExportIndex, InstancePre};
use wasmtime::UpdateDeadline;
use wasmtime::{component::Val, Engine};

#[derive(Clone, Debug)]
pub struct ActivityConfig {
    pub config_id: ConfigId,
    pub forward_stdout: Option<StdOutput>,
    pub forward_stderr: Option<StdOutput>,
    pub env_vars: Arc<[EnvVar]>,
}

pub const TIMEOUT_SLEEP_UNIT: Duration = Duration::from_millis(10);

#[derive(Clone)]
pub struct ActivityWorker<C: ClockFn> {
    engine: Arc<Engine>,
    instance_pre: InstancePre<ActivityCtx>,
    exim: ExIm,
    clock_fn: C,
    exported_ffqn_to_index: hashbrown::HashMap<FunctionFqn, ComponentExportIndex>,
    config_id: ConfigId, // TODO: Move to WorkerContext?
    forward_stdout: Option<StdOutput>,
    forward_stderr: Option<StdOutput>,
    env_vars: Arc<[EnvVar]>,
}

impl<C: ClockFn> ActivityWorker<C> {
    #[tracing::instrument(skip_all, fields(%config.config_id), err)]
    pub fn new_with_config(
        wasm_path: impl AsRef<Path>,
        config: ActivityConfig,
        engine: Arc<Engine>,
        clock_fn: C,
    ) -> Result<Self, WasmFileError> {
        let wasm_path = wasm_path.as_ref();
        let wasm_component =
            WasmComponent::new(wasm_path, &engine).map_err(WasmFileError::DecodeError)?;
        let linking_err = |err: wasmtime::Error| WasmFileError::LinkingError {
            context: StrVariant::Static("linking error"),
            err: err.into(),
        };

        let mut linker = wasmtime::component::Linker::new(&engine);
        // wasi
        wasmtime_wasi::add_to_linker_async(&mut linker).map_err(linking_err)?;
        // wasi-http
        wasmtime_wasi_http::add_only_http_to_linker_async(&mut linker).map_err(linking_err)?;
        // obelisk:log
        log_activities::obelisk::log::log::add_to_linker(&mut linker, |state: &mut ActivityCtx| {
            state
        })
        .map_err(linking_err)?;

        // Attempt to pre-instantiate to catch missing imports
        let instance_pre = linker
            .instantiate_pre(&wasm_component.component)
            .map_err(linking_err)?;

        let exported_ffqn_to_index = wasm_component
            .index_exported_functions()
            .map_err(WasmFileError::DecodeError)?;
        Ok(Self {
            engine,
            exim: wasm_component.exim,
            clock_fn,
            exported_ffqn_to_index,
            config_id: config.config_id,
            forward_stdout: config.forward_stdout,
            forward_stderr: config.forward_stderr,
            env_vars: config.env_vars,
            instance_pre,
        })
    }
}

#[async_trait]
impl<C: ClockFn + 'static> Worker for ActivityWorker<C> {
    fn exported_functions(&self) -> &[FunctionMetadata] {
        &self.exim.exports_flat
    }

    fn imported_functions(&self) -> &[FunctionMetadata] {
        &self.exim.imports_flat
    }

    #[expect(clippy::too_many_lines)]
    async fn run(&self, ctx: WorkerContext) -> WorkerResult {
        trace!("Params: {params:?}", params = ctx.params);
        assert!(ctx.event_history.is_empty());
        let mut store = crate::activity_ctx::store(
            &self.engine,
            ctx.execution_id,
            &self.config_id,
            self.forward_stdout,
            self.forward_stderr,
            &self.env_vars,
            ctx.worker_span.clone(),
        );

        let instance = match self.instance_pre.instantiate_async(&mut store).await {
            Ok(instance) => instance,
            Err(err) => {
                let reason = err.to_string();
                if reason.starts_with("maximum concurrent") {
                    return WorkerResult::Err(WorkerError::LimitReached(reason, ctx.version));
                }
                return WorkerResult::Err(WorkerError::IntermittentError {
                    reason: StrVariant::Arc(Arc::from(format!("cannot instantiate - {err}"))),
                    err: Some(err.into()),
                    version: ctx.version,
                });
            }
        };
        store.epoch_deadline_callback(|_store_ctx| Ok(UpdateDeadline::Yield(1)));
        let fn_export_index = self.exported_ffqn_to_index.get(&ctx.ffqn).expect("executor only calls `run` with ffqns that are exported");
        let Some(func) = instance.get_func(&mut store, fn_export_index) else {
            error!("Cannot unwrap value from `get_func`");
            return WorkerResult::Err(WorkerError::FatalError(
                FatalError::UncategorizedError("cannot unwrap value from `get_func`"),
                ctx.version,
            ));
        };
        let params = match ctx.params.as_vals(func.params(&store)) {
            Ok(params) => params,
            Err(err) => {
                return WorkerResult::Err(WorkerError::FatalError(
                    FatalError::ParamsParsingError(err),
                    ctx.version,
                ));
            }
        };
        let result_types = func.results(&mut store);
        let mut results = vec![Val::Bool(false); result_types.len()];

        let call_function = {
            let worker_span = ctx.worker_span.clone();
            async move {
                if let Err(err) = func.call_async(&mut store, &params, &mut results).await {
                    return WorkerResult::Err(WorkerError::IntermittentError {
                        reason: StrVariant::Arc(Arc::from(format!(
                            "wasm function call error - {err}"
                        ))),
                        err: Some(err.into()),
                        version: ctx.version,
                    });
                }; // guest panic exits here
                let result = match SupportedFunctionReturnValue::new(
                    results.into_iter().zip(result_types.iter().cloned()),
                ) {
                    Ok(result) => result,
                    Err(err) => {
                        return WorkerResult::Err(WorkerError::FatalError(
                            FatalError::ResultParsingError(err),
                            ctx.version,
                        ))
                    }
                };
                if let Err(err) = func.post_return_async(&mut store).await {
                    return WorkerResult::Err(WorkerError::IntermittentError {
                        reason: StrVariant::Arc(Arc::from(format!(
                            "wasm post function call error - {err}"
                        ))),
                        err: Some(err.into()),
                        version: ctx.version,
                    });
                }

                // Interpret `SupportedFunctionResult::Fallible` Err variant as an retry request
                if let Some(exec_err) = result.fallible_err() {
                    if ctx.can_be_retried {
                        let reason = StrVariant::Arc(Arc::from(format!(
                            "Execution returned an error result: `{exec_err:?}`"
                        )));
                        return WorkerResult::Err(WorkerError::IntermittentError {
                            reason,
                            err: None,
                            version: ctx.version,
                        });
                    }
                    // else: log and pass the result as is to be stored.
                    worker_span.in_scope(|| {
                        info!("Execution returned an error result, not able to retry");
                    });
                }
                WorkerResult::Ok(result, ctx.version)
            }
        };
        let started_at = self.clock_fn.now();
        let deadline_delta = ctx.execution_deadline - started_at;
        let deadline_duration = deadline_delta.to_std().unwrap();
        let stopwatch_for_reporting = now_tokio_instant(); // Not using `clock_fn` here is ok, value is only used for log reporting.

        tokio::select! { // future's liveness: Dropping the loser immediately.
            res = call_function => {
                ctx.worker_span
                    .in_scope(||{
                if let WorkerResult::Err(err) = &res {
                    info!(%err, duration = ?stopwatch_for_reporting.elapsed(), ?deadline_duration, execution_deadline = %ctx.execution_deadline, "Finished with an error");
                } else {
                    info!(duration = ?stopwatch_for_reporting.elapsed(), ?deadline_duration,  execution_deadline = %ctx.execution_deadline, "Finished");
                }});
                return res;
            },
            ()  = tokio::time::sleep(deadline_duration) => {
                ctx.worker_span.in_scope(||
                        info!(duration = ?stopwatch_for_reporting.elapsed(), ?deadline_duration, execution_deadline = %ctx.execution_deadline, now = %self.clock_fn.now(), "Timed out")
                    );
                    return WorkerResult::Err(WorkerError::IntermittentTimeout);
            }
        }
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use crate::engines::{EngineConfig, Engines};

    use super::*;
    use assert_matches::assert_matches;
    use concepts::{
        prefixed_ulid::{ExecutorId, RunId},
        storage::{CreateRequest, DbConnection, DbPool, Version},
        ComponentType, ExecutionId, FunctionFqn, Params, SupportedFunctionReturnValue,
    };
    use db_tests::Database;
    use executor::executor::{ExecConfig, ExecTask, ExecutorTaskHandle};
    use serde_json::json;
    use std::time::Duration;
    use test_utils::{env_or_default, sim_clock::SimClock};
    use tracing::info_span;
    use utils::time::Now;
    use val_json::{
        type_wrapper::TypeWrapper,
        wast_val::{WastVal, WastValWithType},
    };

    pub const FIBO_ACTIVITY_FFQN: FunctionFqn = FunctionFqn::new_static_tuple(
        test_programs_fibo_activity_builder::exports::testing::fibo::fibo::FIBO,
    ); // func(n: u8) -> u64;
    pub const FIBO_10_INPUT: u8 = 10;
    pub const FIBO_10_OUTPUT: u64 = 55;

    pub(crate) fn wasm_file_name(input: impl AsRef<Path>) -> StrVariant {
        let input = input.as_ref();
        let input = input.file_name().and_then(|name| name.to_str()).unwrap();
        let input = input.strip_suffix(".wasm").unwrap().to_string();
        StrVariant::from(input)
    }

    fn activity_config(config_id: ConfigId) -> ActivityConfig {
        ActivityConfig {
            config_id,
            forward_stdout: None,
            forward_stderr: None,
            env_vars: Arc::from([]),
        }
    }

    pub(crate) async fn spawn_activity<DB: DbConnection + 'static, P: DbPool<DB> + 'static>(
        db_pool: P,
        wasm_path: &'static str,
        clock_fn: impl ClockFn + 'static,
    ) -> ExecutorTaskHandle {
        let engine = Engines::get_activity_engine(EngineConfig::on_demand_testing().await).unwrap();
        let config_id = ConfigId::new(
            ComponentType::WasmActivity,
            wasm_file_name(wasm_path),
            StrVariant::Static("dummy hash"),
        )
        .unwrap();
        let worker = Arc::new(
            ActivityWorker::new_with_config(
                wasm_path,
                activity_config(config_id.clone()),
                engine,
                clock_fn.clone(),
            )
            .unwrap(),
        );
        let exec_config = ExecConfig {
            batch_size: 1,
            lock_expiry: Duration::from_secs(1),
            tick_sleep: Duration::ZERO,
            config_id,
        };
        ExecTask::spawn_new(
            worker,
            exec_config,
            clock_fn,
            db_pool,
            None,
            ExecutorId::generate(),
        )
    }

    pub(crate) async fn spawn_activity_fibo<DB: DbConnection + 'static, P: DbPool<DB> + 'static>(
        db_pool: P,
        clock_fn: impl ClockFn + 'static,
    ) -> ExecutorTaskHandle {
        spawn_activity(
            db_pool,
            test_programs_fibo_activity_builder::TEST_PROGRAMS_FIBO_ACTIVITY,
            clock_fn,
        )
        .await
    }

    #[tokio::test]
    async fn fibo_once() {
        test_utils::set_up();
        let sim_clock = SimClock::default();
        let (_guard, db_pool) = Database::Memory.set_up().await;
        let db_connection = db_pool.connection();
        let exec_task = spawn_activity_fibo(db_pool.clone(), sim_clock.clone()).await;
        // Create an execution.
        let execution_id = ExecutionId::generate();
        let created_at = sim_clock.now();
        let params = Params::from_json_value(json!([FIBO_10_INPUT])).unwrap();
        db_connection
            .create(CreateRequest {
                created_at,
                execution_id,
                ffqn: FIBO_ACTIVITY_FFQN,
                params,
                parent: None,
                metadata: concepts::ExecutionMetadata::empty(),
                scheduled_at: created_at,
                retry_exp_backoff: Duration::ZERO,
                max_retries: 0,
                config_id: ConfigId::dummy(),
                return_type: None,
                topmost_parent: execution_id,
            })
            .await
            .unwrap();
        // Check the result.
        let fibo = assert_matches!(db_connection.wait_for_finished_result(execution_id, None).await.unwrap(),
            Ok(SupportedFunctionReturnValue::Infallible(WastValWithType {value: WastVal::U64(val), r#type: TypeWrapper::U64 })) => val);
        assert_eq!(FIBO_10_OUTPUT, fibo);
        drop(db_connection);
        exec_task.close().await;
        db_pool.close().await.unwrap();
    }

    #[tokio::test]
    async fn limit_reached() {
        const FIBO_INPUT: u8 = 10;
        const LOCK_EXPIRY_MILLIS: u64 = 1100;
        const TASKS: u32 = 10;
        const MAX_INSTANCES: u32 = 1;

        test_utils::set_up();
        let fibo_input = env_or_default("FIBO_INPUT", FIBO_INPUT);
        let lock_expiry =
            Duration::from_millis(env_or_default("LOCK_EXPIRY_MILLIS", LOCK_EXPIRY_MILLIS));
        let tasks = env_or_default("TASKS", TASKS);
        let max_instances = env_or_default("MAX_INSTANCES", MAX_INSTANCES);

        let mut pool = wasmtime::PoolingAllocationConfig::default();
        pool.total_component_instances(max_instances);
        pool.total_stacks(max_instances);
        pool.total_core_instances(max_instances);
        pool.total_memories(max_instances);
        pool.total_tables(max_instances);

        let engine = Engines::get_activity_engine(EngineConfig::NoCache(
            wasmtime::InstanceAllocationStrategy::Pooling(pool),
        ))
        .unwrap();

        let fibo_worker = ActivityWorker::new_with_config(
            test_programs_fibo_activity_builder::TEST_PROGRAMS_FIBO_ACTIVITY,
            activity_config(ConfigId::dummy()),
            engine,
            Now,
        )
        .unwrap();
        let execution_deadline = Now.now() + lock_expiry;
        // create executions
        let join_handles = (0..tasks)
            .map(|_| {
                let fibo_worker = fibo_worker.clone();
                let execution_id = ExecutionId::generate();
                let ctx = WorkerContext {
                    execution_id,
                    metadata: concepts::ExecutionMetadata::empty(),
                    ffqn: FIBO_ACTIVITY_FFQN,
                    params: Params::from_json_value(json!([fibo_input])).unwrap(),
                    event_history: Vec::new(),
                    responses: Vec::new(),
                    version: Version::new(0),
                    execution_deadline,
                    can_be_retried: false,
                    run_id: RunId::generate(),
                    worker_span: info_span!("worker-test"),
                    topmost_parent: execution_id,
                };
                tokio::spawn(async move { fibo_worker.run(ctx).await })
            })
            .collect::<Vec<_>>();
        let mut limit_reached = 0;
        for jh in join_handles {
            if matches!(
                jh.await.unwrap(),
                WorkerResult::Err(WorkerError::LimitReached(..))
            ) {
                limit_reached += 1;
            }
        }
        assert!(limit_reached > 0, "Limit was not reached");
    }

    #[cfg(not(madsim))] // Requires madsim support in wasmtime
    pub mod wasmtime_nosim {
        use super::*;
        use concepts::storage::ExecutionEventInner;
        use test_utils::sim_clock::SimClock;
        use tracing::{debug, info};

        const EPOCH_MILLIS: u64 = 10;

        pub const SLEEP_LOOP_ACTIVITY_FFQN: FunctionFqn = FunctionFqn::new_static_tuple(
            test_programs_sleep_activity_builder::exports::testing::sleep::sleep::SLEEP_LOOP,
        ); // sleep-loop: func(millis: u64, iterations: u32);
        pub const HTTP_GET_SUCCESSFUL_ACTIVITY :FunctionFqn = FunctionFqn::new_static_tuple(
            test_programs_http_get_activity_builder::exports::testing::http::http_get::GET_SUCCESSFUL,
        );

        #[rstest::rstest]
        #[case(10, 100, Err(concepts::FinishedExecutionError::PermanentTimeout))] // 1s -> timeout
        #[case(10, 10, Ok(SupportedFunctionReturnValue::None))] // 0.1s -> Ok
        #[case(1500, 1, Err(concepts::FinishedExecutionError::PermanentTimeout))] // 1s -> timeout
        #[tokio::test]
        async fn sleep_should_produce_intermittent_timeout(
            #[case] sleep_millis: u32,
            #[case] sleep_iterations: u32,
            #[case] expected: concepts::FinishedExecutionResult,
        ) {
            const LOCK_EXPIRY: Duration = Duration::from_millis(500);
            const TICK_SLEEP: Duration = Duration::from_millis(10);
            test_utils::set_up();
            let (_guard, db_pool) = Database::Memory.set_up().await;
            let timers_watcher_task = executor::expired_timers_watcher::spawn_new(
                db_pool.clone(),
                executor::expired_timers_watcher::TimersWatcherConfig {
                    tick_sleep: TICK_SLEEP,
                    clock_fn: Now,
                },
            );
            let engine =
                Engines::get_activity_engine(EngineConfig::on_demand_testing().await).unwrap();
            let _epoch_ticker = crate::epoch_ticker::EpochTicker::spawn_new(
                vec![engine.weak()],
                Duration::from_millis(EPOCH_MILLIS),
            );

            let worker = Arc::new(
                ActivityWorker::new_with_config(
                    test_programs_sleep_activity_builder::TEST_PROGRAMS_SLEEP_ACTIVITY,
                    activity_config(ConfigId::dummy()),
                    engine,
                    Now,
                )
                .unwrap(),
            );

            let exec_config = ExecConfig {
                batch_size: 1,
                lock_expiry: LOCK_EXPIRY,
                tick_sleep: TICK_SLEEP,
                config_id: ConfigId::dummy(),
            };
            let exec_task = ExecTask::spawn_new(
                worker,
                exec_config,
                Now,
                db_pool.clone(),
                None,
                ExecutorId::generate(),
            );

            // Create an execution.
            let stopwatch = std::time::Instant::now();
            let execution_id = ExecutionId::generate();
            info!("Testing {execution_id}");
            let created_at = Now.now();
            let db_connection = db_pool.connection();
            db_connection
                .create(CreateRequest {
                    created_at,
                    execution_id,
                    ffqn: SLEEP_LOOP_ACTIVITY_FFQN,
                    params: Params::from_json_value(json!([
                        sleep_millis * 1_000_000,
                        sleep_iterations
                    ]))
                    .unwrap(),
                    parent: None,
                    metadata: concepts::ExecutionMetadata::empty(),
                    scheduled_at: created_at,
                    retry_exp_backoff: Duration::ZERO,
                    max_retries: 0,
                    config_id: ConfigId::dummy(),
                    return_type: None,
                    topmost_parent: execution_id,
                })
                .await
                .unwrap();
            // Check the result.
            assert_eq!(
                expected,
                assert_matches!(
                    db_connection
                        .wait_for_finished_result(execution_id, Some(Duration::from_secs(1)))
                        .await
                        .unwrap(),
                    actual => actual
                )
            );
            let stopwatch = stopwatch.elapsed();
            info!("Finished in {stopwatch:?}");
            assert!(stopwatch < LOCK_EXPIRY * 2);

            drop(db_connection);
            timers_watcher_task.close().await;
            exec_task.close().await;
            db_pool.close().await.unwrap();
        }

        #[rstest::rstest]
        #[case(1, 2_000)] // 1ms * 2000 iterations
        #[case(2_000, 1)] // 2s * 1 iteration
        #[tokio::test]
        async fn long_running_execution_should_timeout(
            #[case] sleep_millis: u64,
            #[case] sleep_iterations: u32,
        ) {
            const TIMEOUT: Duration = Duration::from_millis(200);
            test_utils::set_up();

            let engine =
                Engines::get_activity_engine(EngineConfig::on_demand_testing().await).unwrap();
            let _epoch_ticker = crate::epoch_ticker::EpochTicker::spawn_new(
                vec![engine.weak()],
                Duration::from_millis(EPOCH_MILLIS),
            );

            let worker = Arc::new(
                ActivityWorker::new_with_config(
                    test_programs_sleep_activity_builder::TEST_PROGRAMS_SLEEP_ACTIVITY,
                    activity_config(ConfigId::dummy()),
                    engine,
                    Now,
                )
                .unwrap(),
            );

            let executed_at = Now.now();
            let execution_id = ExecutionId::generate();
            let ctx = WorkerContext {
                execution_id: ExecutionId::generate(),
                metadata: concepts::ExecutionMetadata::empty(),
                ffqn: SLEEP_LOOP_ACTIVITY_FFQN,
                params: Params::from_json_value(json!([
                    sleep_millis * 1_000_000,
                    sleep_iterations
                ]))
                .unwrap(),
                event_history: Vec::new(),
                responses: Vec::new(),
                version: Version::new(0),
                execution_deadline: executed_at + TIMEOUT,
                can_be_retried: false,
                run_id: RunId::generate(),
                worker_span: info_span!("worker-test"),
                topmost_parent: execution_id,
            };
            let WorkerResult::Err(err) = worker.run(ctx).await else {
                panic!()
            };
            assert_matches!(err, WorkerError::IntermittentTimeout);
        }

        #[tokio::test]
        #[allow(clippy::too_many_lines)]
        async fn http_get_simple() {
            use std::ops::Deref;
            use wiremock::{
                matchers::{method, path},
                Mock, MockServer, ResponseTemplate,
            };
            const BODY: &str = "ok";
            const RETRY_EXP_BACKOFF: Duration = Duration::from_millis(10);
            test_utils::set_up();
            info!("All set up");
            let sim_clock = SimClock::default();
            let (_guard, db_pool) = Database::Memory.set_up().await;
            let engine =
                Engines::get_activity_engine(EngineConfig::on_demand_testing().await).unwrap();
            let worker = Arc::new(
                ActivityWorker::new_with_config(
                    test_programs_http_get_activity_builder::TEST_PROGRAMS_HTTP_GET_ACTIVITY,
                    activity_config(ConfigId::dummy()),
                    engine,
                    sim_clock.clone(),
                )
                .unwrap(),
            );
            let exec_config = ExecConfig {
                batch_size: 1,
                lock_expiry: Duration::from_secs(1),
                tick_sleep: Duration::ZERO,
                config_id: ConfigId::dummy(),
            };
            let ffqns = Arc::from([HTTP_GET_SUCCESSFUL_ACTIVITY]);
            let exec_task = ExecTask::new(
                worker,
                exec_config,
                sim_clock.clone(),
                db_pool.clone(),
                ffqns,
            );

            let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
            let server_address = listener
                .local_addr()
                .expect("Failed to get server address.");

            let params = Params::from_json_value(json!([format!(
                "http://127.0.0.1:{port}/",
                port = server_address.port()
            )]))
            .unwrap();
            let execution_id = ExecutionId::generate();
            let created_at = sim_clock.now();
            let db_connection = db_pool.connection();
            info!("Creating execution");
            let stopwatch = std::time::Instant::now();
            db_connection
                .create(CreateRequest {
                    created_at,
                    execution_id,
                    ffqn: HTTP_GET_SUCCESSFUL_ACTIVITY,
                    params,
                    parent: None,
                    metadata: concepts::ExecutionMetadata::empty(),
                    scheduled_at: created_at,
                    retry_exp_backoff: RETRY_EXP_BACKOFF,
                    max_retries: 1,
                    config_id: ConfigId::dummy(),
                    return_type: None,
                    topmost_parent: execution_id,
                })
                .await
                .unwrap();

            let server = MockServer::builder().listener(listener).start().await;
            Mock::given(method("GET"))
                .and(path("/"))
                .respond_with(ResponseTemplate::new(200).set_body_string(BODY))
                .expect(1)
                .mount(&server)
                .await;

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
            let exec_log = db_connection.get(execution_id).await.unwrap();
            let stopwatch = stopwatch.elapsed();
            info!("Finished in {stopwatch:?}");
            let res = assert_matches!(exec_log.last_event().event.clone(), ExecutionEventInner::Finished { result } => result);
            let res = res.unwrap();
            let wast_val = assert_matches!(res.fallible_ok(), Some(Some(wast_val)) => wast_val);
            let val = assert_matches!(wast_val, WastVal::String(val) => val);
            assert_eq!(BODY, val.deref());
            // check types
            let (ok, err) = assert_matches!(res, SupportedFunctionReturnValue::Fallible(WastValWithType{value: _,
                r#type: TypeWrapper::Result{ok, err}}) => (ok, err));
            assert_eq!(Some(Box::new(TypeWrapper::String)), ok);
            assert_eq!(Some(Box::new(TypeWrapper::String)), err);
            drop(db_connection);
            drop(exec_task);
            db_pool.close().await.unwrap();
        }

        #[rstest::rstest(
            succeed_eventually => [false, true],
        )]
        #[tokio::test]
        async fn http_get_retry_on_fallible_err(succeed_eventually: bool) {
            use std::ops::Deref;
            use wiremock::{
                matchers::{method, path},
                Mock, MockServer, ResponseTemplate,
            };
            const BODY: &str = "ok";
            const RETRY_EXP_BACKOFF: Duration = Duration::from_millis(10);
            test_utils::set_up();
            let sim_clock = SimClock::default();
            let (_guard, db_pool) = Database::Memory.set_up().await;
            let engine =
                Engines::get_activity_engine(EngineConfig::on_demand_testing().await).unwrap();
            let worker = Arc::new(
                ActivityWorker::new_with_config(
                    test_programs_http_get_activity_builder::TEST_PROGRAMS_HTTP_GET_ACTIVITY,
                    activity_config(ConfigId::dummy()),
                    engine,
                    sim_clock.clone(),
                )
                .unwrap(),
            );
            let exec_config = ExecConfig {
                batch_size: 1,
                lock_expiry: Duration::from_secs(1),
                tick_sleep: Duration::ZERO,
                config_id: ConfigId::dummy(),
            };
            let ffqns = Arc::from([HTTP_GET_SUCCESSFUL_ACTIVITY]);
            let exec_task = ExecTask::new(
                worker,
                exec_config,
                sim_clock.clone(),
                db_pool.clone(),
                ffqns,
            );

            let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
            let server_address = listener
                .local_addr()
                .expect("Failed to get server address.");

            let params = Params::from_json_value(json!([format!(
                "http://127.0.0.1:{port}/",
                port = server_address.port()
            )]))
            .unwrap();
            let execution_id = ExecutionId::generate();
            let created_at = sim_clock.now();
            let db_connection = db_pool.connection();
            db_connection
                .create(CreateRequest {
                    created_at,
                    execution_id,
                    ffqn: HTTP_GET_SUCCESSFUL_ACTIVITY,
                    params,
                    parent: None,
                    metadata: concepts::ExecutionMetadata::empty(),
                    scheduled_at: created_at,
                    retry_exp_backoff: RETRY_EXP_BACKOFF,
                    max_retries: 1,
                    config_id: ConfigId::dummy(),
                    return_type: None,
                    topmost_parent: execution_id,
                })
                .await
                .unwrap();

            let server = MockServer::builder().listener(listener).start().await;
            Mock::given(method("GET"))
                .and(path("/"))
                .respond_with(ResponseTemplate::new(500).set_body_string(BODY))
                .expect(1)
                .mount(&server)
                .await;
            debug!("started mock server on {}", server.address());

            {
                // Expect error result to be interpreted as an intermittent failure
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
                let exec_log = db_connection.get(execution_id).await.unwrap();

                let (reason, found_expires_at) = assert_matches!(
                    &exec_log.last_event().event,
                    ExecutionEventInner::IntermittentFailure {
                        expires_at,
                        reason,
                    }
                    => (reason, *expires_at)
                );
                assert_eq!(sim_clock.now() + RETRY_EXP_BACKOFF, found_expires_at);
                assert!(
                    reason.contains("wrong status code: 500"),
                    "Unexpected {reason}"
                );
                server.verify().await;
            }
            // Noop until the timeout expires
            assert_eq!(
                0,
                exec_task
                    .tick2(sim_clock.now())
                    .await
                    .unwrap()
                    .wait_for_tasks()
                    .await
                    .unwrap()
            );
            sim_clock.move_time_forward(RETRY_EXP_BACKOFF).await;
            server.reset().await;
            if succeed_eventually {
                // Reconfigure the server
                Mock::given(method("GET"))
                    .and(path("/"))
                    .respond_with(ResponseTemplate::new(200).set_body_string(BODY))
                    .expect(1)
                    .mount(&server)
                    .await;
                debug!("Reconfigured the server");
            } // otherwise return 404

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
            let exec_log = db_connection.get(execution_id).await.unwrap();
            let res = assert_matches!(exec_log.last_event().event.clone(), ExecutionEventInner::Finished { result } => result);
            let res = res.unwrap();
            if succeed_eventually {
                let wast_val = assert_matches!(res.fallible_ok(), Some(Some(wast_val)) => wast_val);
                let val = assert_matches!(wast_val, WastVal::String(val) => val);
                assert_eq!(BODY, val.deref());
            } else {
                let wast_val =
                    assert_matches!(res.fallible_err(), Some(Some(wast_val)) => wast_val);
                let val = assert_matches!(wast_val, WastVal::String(val) => val);
                assert_eq!("wrong status code: 404", val.deref());
            }
            // check types
            let (ok, err) = assert_matches!(res, SupportedFunctionReturnValue::Fallible(WastValWithType{value: _,
                r#type: TypeWrapper::Result{ok, err}}) => (ok, err));
            assert_eq!(Some(Box::new(TypeWrapper::String)), ok);
            assert_eq!(Some(Box::new(TypeWrapper::String)), err);
            drop(db_connection);
            drop(exec_task);
            db_pool.close().await.unwrap();
        }
    }
}
