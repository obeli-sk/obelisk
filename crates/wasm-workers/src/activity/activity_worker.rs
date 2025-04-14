use super::activity_ctx::{self, ActivityCtx};
use crate::WasmFileError;
use crate::activity::activity_ctx::HttpClientTracesContainer;
use crate::component_logger::log_activities;
use crate::envvar::EnvVar;
use crate::std_output_stream::StdOutput;
use async_trait::async_trait;
use concepts::storage::http_client_trace::HttpClientTrace;
use concepts::time::{ClockFn, Sleep, now_tokio_instant};
use concepts::{ComponentId, FunctionFqn, PackageIfcFns, SupportedFunctionReturnValue, TrapKind};
use concepts::{FunctionMetadata, StrVariant};
use executor::worker::{FatalError, WorkerContext, WorkerResult};
use executor::worker::{Worker, WorkerError};
use itertools::Itertools;
use std::{fmt::Debug, sync::Arc};
use tracing::{info, trace};
use utils::wasm_tools::{ExIm, WasmComponent};
use wasmtime::UpdateDeadline;
use wasmtime::component::{ComponentExportIndex, InstancePre};
use wasmtime::{Engine, component::Val};

#[derive(Clone, Debug)]
pub struct ActivityConfig {
    pub component_id: ComponentId,
    pub forward_stdout: Option<StdOutput>,
    pub forward_stderr: Option<StdOutput>,
    pub env_vars: Arc<[EnvVar]>,
    pub retry_on_err: bool,
}

#[derive(Clone)]
pub struct ActivityWorker<C: ClockFn, S: Sleep> {
    engine: Arc<Engine>,
    instance_pre: InstancePre<ActivityCtx<C>>,
    exim: ExIm,
    clock_fn: C,
    sleep: S,
    exported_ffqn_to_index: hashbrown::HashMap<FunctionFqn, ComponentExportIndex>,
    config: ActivityConfig,
}

impl<C: ClockFn + 'static, S: Sleep> ActivityWorker<C, S> {
    #[tracing::instrument(skip_all, fields(%config.component_id), err)]
    pub fn new_with_config(
        wasm_component: WasmComponent,
        config: ActivityConfig,
        engine: Arc<Engine>,
        clock_fn: C,
        sleep: S,
    ) -> Result<Self, WasmFileError> {
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
        log_activities::obelisk::log::log::add_to_linker(
            &mut linker,
            |state: &mut ActivityCtx<C>| state,
        )
        .map_err(linking_err)?;

        // Attempt to pre-instantiate to catch missing imports
        let instance_pre = linker
            .instantiate_pre(&wasm_component.wasmtime_component)
            .map_err(linking_err)?;

        let exported_ffqn_to_index = wasm_component
            .index_exported_functions()
            .map_err(WasmFileError::DecodeError)?;
        Ok(Self {
            engine,
            exim: wasm_component.exim,
            clock_fn,
            sleep,
            exported_ffqn_to_index,
            config,
            instance_pre,
        })
    }

    pub fn exported_functions_ext(&self) -> &[FunctionMetadata] {
        self.exim.get_exports(true)
    }

    pub fn exports_hierarchy_ext(&self) -> &[PackageIfcFns] {
        self.exim.get_exports_hierarchy_ext()
    }
}

#[async_trait]
impl<C: ClockFn + 'static, S: Sleep + 'static> Worker for ActivityWorker<C, S> {
    fn exported_functions(&self) -> &[FunctionMetadata] {
        self.exim.get_exports(false)
    }

    fn imported_functions(&self) -> &[FunctionMetadata] {
        &self.exim.imports_flat
    }

    #[expect(clippy::too_many_lines)]
    async fn run(&self, ctx: WorkerContext) -> WorkerResult {
        trace!("Params: {params:?}", params = ctx.params);
        assert!(ctx.event_history.is_empty());
        let http_client_traces = HttpClientTracesContainer::default();
        let mut store = activity_ctx::store(
            &self.engine,
            &ctx.execution_id,
            &self.config,
            ctx.worker_span.clone(),
            self.clock_fn.clone(),
            http_client_traces.clone(),
        );

        let instance = match self.instance_pre.instantiate_async(&mut store).await {
            Ok(instance) => instance,
            Err(err) => {
                let reason = err.to_string();
                if reason.starts_with("maximum concurrent") {
                    return WorkerResult::Err(WorkerError::LimitReached {
                        reason,
                        version: ctx.version,
                    });
                }
                return WorkerResult::Err(WorkerError::FatalError(
                    FatalError::CannotInstantiate {
                        reason: format!("{err}"),
                        detail: format!("{err:?}"),
                    },
                    ctx.version,
                ));
            }
        };
        store.epoch_deadline_callback(|_store_ctx| Ok(UpdateDeadline::Yield(1)));
        let fn_export_index = self
            .exported_ffqn_to_index
            .get(&ctx.ffqn)
            .expect("executor only calls `run` with ffqns that are exported");
        let func = instance
            .get_func(&mut store, fn_export_index)
            .expect("exported function must be found");

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
        let retry_on_err = self.config.retry_on_err;
        let call_function = {
            let worker_span = ctx.worker_span.clone();
            let http_client_traces = http_client_traces.clone();
            let version = ctx.version.clone();
            async move {
                let res = func.call_async(&mut store, &params, &mut results).await;
                let http_client_traces = http_client_traces
                    .lock()
                    .unwrap()
                    .drain(..)
                    .map(|(req, mut resp)| HttpClientTrace {
                        req,
                        resp: resp.try_recv().ok(),
                    })
                    .collect_vec();
                if let Err(err) = res {
                    return WorkerResult::Err(WorkerError::ActivityTrap {
                        reason: err.to_string(),
                        trap_kind: TrapKind::Trap,
                        detail: format!("{err:?}"),
                        version,
                        http_client_traces: Some(http_client_traces),
                    });
                };
                let result = match SupportedFunctionReturnValue::new(
                    results.into_iter().zip(result_types.iter().cloned()),
                ) {
                    Ok(result) => result,
                    Err(err) => {
                        return WorkerResult::Err(WorkerError::FatalError(
                            FatalError::ResultParsingError(err),
                            version,
                        ));
                    }
                };

                if let Err(err) = func.post_return_async(&mut store).await {
                    return WorkerResult::Err(WorkerError::ActivityTrap {
                        reason: err.to_string(),
                        trap_kind: TrapKind::PostReturnTrap,
                        detail: format!("{err:?}"),
                        version,
                        http_client_traces: Some(http_client_traces),
                    });
                }

                if retry_on_err {
                    // Interpret any `SupportedFunctionResult::Fallible` Err variant as an retry request (TemporaryError)
                    if let SupportedFunctionReturnValue::FallibleResultErr(result_err) = &result {
                        if ctx.can_be_retried {
                            let detail = serde_json::to_string(result_err).expect(
                                "SupportedFunctionReturnValue should be serializable to JSON",
                            );
                            return WorkerResult::Err(WorkerError::ActivityReturnedError {
                                detail: Some(detail),
                                version,
                                http_client_traces: Some(http_client_traces),
                            });
                        }
                        // else: log and pass the retval as is to be stored.
                        worker_span.in_scope(|| {
                            info!("Execution returned `result::err`, not able to retry");
                        });
                    }
                }
                WorkerResult::Ok(result, version, Some(http_client_traces))
            }
        };
        let started_at = self.clock_fn.now();
        let deadline_delta = ctx.execution_deadline - started_at;
        let Ok(deadline_duration) = deadline_delta.to_std() else {
            ctx.worker_span.in_scope(||
                info!(execution_deadline = %ctx.execution_deadline, %started_at, "Timed out - started_at later than execution_deadline")
            );
            return WorkerResult::Err(WorkerError::TemporaryTimeout {
                http_client_traces: None,
                version: ctx.version,
            });
        };
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
            ()  = self.sleep.sleep(deadline_duration) => {
                ctx.worker_span.in_scope(||
                        info!(duration = ?stopwatch_for_reporting.elapsed(), %started_at, ?deadline_duration, execution_deadline = %ctx.execution_deadline, now = %self.clock_fn.now(), "Timed out")
                    );
                let http_client_traces = http_client_traces
                    .lock()
                    .unwrap()
                    .drain(..)
                        .map(|(req, mut resp)| HttpClientTrace {
                            req,
                            resp: resp.try_recv().ok(),
                        })
                        .collect_vec();
                return WorkerResult::Err(WorkerError::TemporaryTimeout{
                    http_client_traces: Some(http_client_traces),
                    version: ctx.version,
                });
            }
        }
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::engines::{EngineConfig, Engines};
    use assert_matches::assert_matches;
    use concepts::time::TokioSleep;
    use concepts::{
        ComponentType, ExecutionId, FunctionFqn, Params, SupportedFunctionReturnValue,
        prefixed_ulid::ExecutorId,
        storage::{CreateRequest, DbConnection, DbPool},
    };
    use db_tests::Database;
    use executor::executor::{ExecConfig, ExecTask, ExecutorTaskHandle};
    use serde_json::json;
    use std::{path::Path, time::Duration};
    use test_utils::sim_clock::SimClock;
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

    fn activity_config(component_id: ComponentId) -> ActivityConfig {
        ActivityConfig {
            component_id,
            forward_stdout: None,
            forward_stderr: None,
            env_vars: Arc::from([]),
            retry_on_err: true,
        }
    }

    pub(crate) async fn compile_activity(wasm_path: &str) -> (WasmComponent, ComponentId) {
        let engine = Engines::get_activity_engine(EngineConfig::on_demand_testing().await).unwrap();
        compile_activity_with_engine(wasm_path, &engine)
    }

    pub(crate) fn compile_activity_with_engine(
        wasm_path: &str,
        engine: &Engine,
    ) -> (WasmComponent, ComponentId) {
        let component_id =
            ComponentId::new(ComponentType::ActivityWasm, wasm_file_name(wasm_path)).unwrap();
        (
            WasmComponent::new(wasm_path, engine, Some(component_id.component_type.into()))
                .unwrap(),
            component_id,
        )
    }

    fn new_activity_worker(
        wasm_path: &str,
        engine: Arc<Engine>,
        clock_fn: impl ClockFn + 'static,
        sleep: impl Sleep + 'static,
    ) -> (Arc<dyn Worker>, ComponentId) {
        let (wasm_component, component_id) = compile_activity_with_engine(wasm_path, &engine);
        (
            Arc::new(
                ActivityWorker::new_with_config(
                    wasm_component,
                    activity_config(component_id.clone()),
                    engine,
                    clock_fn,
                    sleep,
                )
                .unwrap(),
            ),
            component_id,
        )
    }

    pub(crate) async fn spawn_activity<DB: DbConnection + 'static, P: DbPool<DB> + 'static>(
        db_pool: P,
        wasm_path: &'static str,
        clock_fn: impl ClockFn + 'static,
        sleep: impl Sleep + 'static,
    ) -> ExecutorTaskHandle {
        let engine = Engines::get_activity_engine(EngineConfig::on_demand_testing().await).unwrap();
        let (worker, component_id) =
            new_activity_worker(wasm_path, engine, clock_fn.clone(), sleep);
        let exec_config = ExecConfig {
            batch_size: 1,
            lock_expiry: Duration::from_secs(1),
            tick_sleep: Duration::ZERO,
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

    pub(crate) async fn spawn_activity_fibo<DB: DbConnection + 'static, P: DbPool<DB> + 'static>(
        db_pool: P,
        clock_fn: impl ClockFn + 'static,
        sleep: impl Sleep + 'static,
    ) -> ExecutorTaskHandle {
        spawn_activity(
            db_pool,
            test_programs_fibo_activity_builder::TEST_PROGRAMS_FIBO_ACTIVITY,
            clock_fn,
            sleep,
        )
        .await
    }

    #[tokio::test]
    async fn fibo_once() {
        test_utils::set_up();
        let sim_clock = SimClock::default();
        let (_guard, db_pool) = Database::Memory.set_up().await;
        let db_connection = db_pool.connection();
        let exec_task = spawn_activity_fibo(db_pool.clone(), sim_clock.clone(), TokioSleep).await;
        // Create an execution.
        let execution_id = ExecutionId::generate();
        let created_at = sim_clock.now();
        let params = Params::from_json_values(vec![json!(FIBO_10_INPUT)]);
        db_connection
            .create(CreateRequest {
                created_at,
                execution_id: execution_id.clone(),
                ffqn: FIBO_ACTIVITY_FFQN,
                params,
                parent: None,
                metadata: concepts::ExecutionMetadata::empty(),
                scheduled_at: created_at,
                retry_exp_backoff: Duration::ZERO,
                max_retries: 0,
                component_id: ComponentId::dummy_activity(),
                scheduled_by: None,
            })
            .await
            .unwrap();
        // Check the result.
        let fibo = assert_matches!(db_connection.wait_for_finished_result(&execution_id, None).await.unwrap(),
            Ok(SupportedFunctionReturnValue::InfallibleOrResultOk(WastValWithType {value: WastVal::U64(val), r#type: TypeWrapper::U64 })) => val);
        assert_eq!(FIBO_10_OUTPUT, fibo);
        drop(db_connection);
        exec_task.close().await;
        db_pool.close().await.unwrap();
    }

    #[cfg(not(madsim))] // Requires madsim support in wasmtime
    pub mod wasmtime_nosim {
        use super::*;
        use crate::engines::PoolingOptions;
        use concepts::storage::http_client_trace::{RequestTrace, ResponseTrace};
        use concepts::time::Now;
        use concepts::{FinishedExecutionError, PermanentFailureKind};
        use concepts::{
            prefixed_ulid::RunId,
            storage::{ExecutionEventInner, Version},
        };
        use test_utils::{env_or_default, sim_clock::SimClock};
        use tracing::{debug, info, info_span};

        const EPOCH_MILLIS: u64 = 10;

        pub const SLEEP_LOOP_ACTIVITY_FFQN: FunctionFqn = FunctionFqn::new_static_tuple(
            test_programs_sleep_activity_builder::exports::testing::sleep::sleep::SLEEP_LOOP,
        ); // sleep-loop: func(millis: u64, iterations: u32);
        pub const HTTP_GET_SUCCESSFUL_ACTIVITY :FunctionFqn = FunctionFqn::new_static_tuple(
            test_programs_http_get_activity_builder::exports::testing::http::http_get::GET_SUCCESSFUL,
        );

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

            let pool_opts = PoolingOptions {
                pooling_total_component_instances: Some(max_instances),
                pooling_total_stacks: Some(max_instances),
                pooling_total_core_instances: Some(max_instances),
                pooling_total_memories: Some(max_instances),
                pooling_total_tables: Some(max_instances),
                ..Default::default()
            };

            let engine =
                Engines::get_activity_engine(EngineConfig::pooling_nocache_testing(pool_opts))
                    .unwrap();

            let (fibo_worker, _) = new_activity_worker(
                test_programs_fibo_activity_builder::TEST_PROGRAMS_FIBO_ACTIVITY,
                engine,
                Now,
                TokioSleep,
            );
            let execution_deadline = Now.now() + lock_expiry;
            // create executions
            let join_handles = (0..tasks)
                .map(|_| {
                    let fibo_worker = fibo_worker.clone();
                    let execution_id = ExecutionId::generate();
                    let ctx = WorkerContext {
                        execution_id: execution_id.clone(),
                        metadata: concepts::ExecutionMetadata::empty(),
                        ffqn: FIBO_ACTIVITY_FFQN,
                        params: Params::from_json_values(vec![json!(fibo_input)]),
                        event_history: Vec::new(),
                        responses: Vec::new(),
                        version: Version::new(0),
                        execution_deadline,
                        can_be_retried: false,
                        run_id: RunId::generate(),
                        worker_span: info_span!("worker-test"),
                    };
                    tokio::spawn(async move { fibo_worker.run(ctx).await })
                })
                .collect::<Vec<_>>();
            let mut limit_reached = 0;
            for jh in join_handles {
                if matches!(
                    jh.await.unwrap(),
                    WorkerResult::Err(WorkerError::LimitReached { .. })
                ) {
                    limit_reached += 1;
                }
            }
            assert!(limit_reached > 0, "Limit was not reached");
        }

        #[rstest::rstest]
        #[case(10, 100, Err(concepts::FinishedExecutionError::PermanentTimeout))] // 1s -> timeout
        #[case(10, 10, Ok(SupportedFunctionReturnValue::None))] // 0.1s -> Ok
        #[case(1500, 1, Err(concepts::FinishedExecutionError::PermanentTimeout))] // 1s -> timeout
        #[tokio::test]
        async fn flaky_sleep_should_produce_temporary_timeout(
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
                    leeway: Duration::ZERO,
                },
            );
            let engine =
                Engines::get_activity_engine(EngineConfig::on_demand_testing().await).unwrap();
            let _epoch_ticker = crate::epoch_ticker::EpochTicker::spawn_new(
                vec![engine.weak()],
                Duration::from_millis(EPOCH_MILLIS),
            );

            let (worker, _) = new_activity_worker(
                test_programs_sleep_activity_builder::TEST_PROGRAMS_SLEEP_ACTIVITY,
                engine,
                Now,
                TokioSleep,
            );

            let exec_config = ExecConfig {
                batch_size: 1,
                lock_expiry: LOCK_EXPIRY,
                tick_sleep: TICK_SLEEP,
                component_id: ComponentId::dummy_activity(),
                task_limiter: None,
            };
            let exec_task = ExecTask::spawn_new(
                worker,
                exec_config,
                Now,
                db_pool.clone(),
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
                    execution_id: execution_id.clone(),
                    ffqn: SLEEP_LOOP_ACTIVITY_FFQN,
                    params: Params::from_json_values(vec![
                        json!(
                        {"milliseconds": sleep_millis}),
                        json!(sleep_iterations),
                    ]),
                    parent: None,
                    metadata: concepts::ExecutionMetadata::empty(),
                    scheduled_at: created_at,
                    retry_exp_backoff: Duration::ZERO,
                    max_retries: 0,
                    component_id: ComponentId::dummy_activity(),
                    scheduled_by: None,
                })
                .await
                .unwrap();
            // Check the result.
            assert_eq!(
                expected,
                assert_matches!(
                    db_connection
                        .wait_for_finished_result(&execution_id, Some(Duration::from_secs(1)))
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
            let sim_clock = SimClock::epoch();
            let (worker, _) = new_activity_worker(
                test_programs_sleep_activity_builder::TEST_PROGRAMS_SLEEP_ACTIVITY,
                engine,
                sim_clock.clone(),
                TokioSleep,
            );

            let executed_at = sim_clock.now();
            let version = Version::new(10);
            let ctx = WorkerContext {
                execution_id: ExecutionId::generate(),
                metadata: concepts::ExecutionMetadata::empty(),
                ffqn: SLEEP_LOOP_ACTIVITY_FFQN,
                params: Params::from_json_values(vec![
                    json!(
                    {"milliseconds": sleep_millis}),
                    json!(sleep_iterations),
                ]),
                event_history: Vec::new(),
                responses: Vec::new(),
                version: version.clone(),
                execution_deadline: executed_at + TIMEOUT,
                can_be_retried: false,
                run_id: RunId::generate(),
                worker_span: info_span!("worker-test"),
            };
            let WorkerResult::Err(err) = worker.run(ctx).await else {
                panic!()
            };
            let (http_client_traces, actual_version) = assert_matches!(
                err,
                WorkerError::TemporaryTimeout {
                    http_client_traces,
                    version
                }
                => (http_client_traces, version)
            );
            assert_eq!(http_client_traces, Some(Vec::new()));
            assert_eq!(version, actual_version);
        }

        #[tokio::test]
        async fn execution_deadline_before_now_should_timeout() {
            test_utils::set_up();

            let engine =
                Engines::get_activity_engine(EngineConfig::on_demand_testing().await).unwrap();
            let sim_clock = SimClock::epoch();
            let (worker, _) = new_activity_worker(
                test_programs_sleep_activity_builder::TEST_PROGRAMS_SLEEP_ACTIVITY,
                engine,
                sim_clock.clone(),
                TokioSleep,
            );
            // simulate a scheduling problem where deadline < now
            let execution_deadline = sim_clock.now();
            sim_clock
                .move_time_forward(Duration::from_millis(100))
                .await;
            let version = Version::new(10);
            let ctx = WorkerContext {
                execution_id: ExecutionId::generate(),
                metadata: concepts::ExecutionMetadata::empty(),
                ffqn: SLEEP_LOOP_ACTIVITY_FFQN,
                params: Params::from_json_values(vec![
                    json!(
                    {"milliseconds": 1}),
                    json!(1),
                ]),
                event_history: Vec::new(),
                responses: Vec::new(),
                version: version.clone(),
                execution_deadline,
                can_be_retried: false,
                run_id: RunId::generate(),
                worker_span: info_span!("worker-test"),
            };
            let WorkerResult::Err(err) = worker.run(ctx).await else {
                panic!()
            };
            let actual_version = assert_matches!(
                err,
                WorkerError::TemporaryTimeout {
                    http_client_traces: None,
                    version: actual_version,
                }
                => actual_version
            );
            assert_eq!(version, actual_version);
        }

        #[expect(clippy::too_many_lines)]
        #[tokio::test]
        async fn http_get_simple() {
            use std::ops::Deref;
            use wiremock::{
                Mock, MockServer, ResponseTemplate,
                matchers::{method, path},
            };
            const BODY: &str = "ok";
            const RETRY_EXP_BACKOFF: Duration = Duration::from_millis(10);
            test_utils::set_up();
            info!("All set up");
            let sim_clock = SimClock::default();
            let (_guard, db_pool) = Database::Memory.set_up().await;
            let engine =
                Engines::get_activity_engine(EngineConfig::on_demand_testing().await).unwrap();
            let (worker, _) = new_activity_worker(
                test_programs_http_get_activity_builder::TEST_PROGRAMS_HTTP_GET_ACTIVITY,
                engine,
                sim_clock.clone(),
                TokioSleep,
            );
            let exec_config = ExecConfig {
                batch_size: 1,
                lock_expiry: Duration::from_secs(1),
                tick_sleep: Duration::ZERO,
                component_id: ComponentId::dummy_activity(),
                task_limiter: None,
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
            let uri = format!("http://127.0.0.1:{port}/", port = server_address.port());
            let params = Params::from_json_values(vec![json!(uri.clone())]);
            let execution_id = ExecutionId::generate();
            let created_at = sim_clock.now();
            let db_connection = db_pool.connection();
            info!("Creating execution");
            let stopwatch = std::time::Instant::now();
            db_connection
                .create(CreateRequest {
                    created_at,
                    execution_id: execution_id.clone(),
                    ffqn: HTTP_GET_SUCCESSFUL_ACTIVITY,
                    params,
                    parent: None,
                    metadata: concepts::ExecutionMetadata::empty(),
                    scheduled_at: created_at,
                    retry_exp_backoff: RETRY_EXP_BACKOFF,
                    max_retries: 1,
                    component_id: ComponentId::dummy_activity(),
                    scheduled_by: None,
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
                    .tick_test(sim_clock.now())
                    .await
                    .unwrap()
                    .wait_for_tasks()
                    .await
                    .unwrap()
            );
            let exec_log = db_connection.get(&execution_id).await.unwrap();
            let stopwatch = stopwatch.elapsed();
            info!("Finished in {stopwatch:?}");
            let (res, http_client_traces) = assert_matches!(
                exec_log.last_event().event.clone(),
                ExecutionEventInner::Finished { result, http_client_traces: Some(http_client_traces) }
                => (result, http_client_traces));
            let res = res.unwrap();
            let wast_val = assert_matches!(res.fallible_ok(), Some(Some(wast_val)) => wast_val);
            let val = assert_matches!(wast_val, WastVal::String(val) => val);
            assert_eq!(BODY, val.deref());
            // check types
            let (ok, err) =
                assert_matches!(res.val_type(), Some(TypeWrapper::Result{ok, err}) => (ok, err));
            assert_eq!(Some(Box::new(TypeWrapper::String)), *ok);
            assert_eq!(Some(Box::new(TypeWrapper::String)), *err);
            assert_eq!(1, http_client_traces.len());
            let http_client_trace = http_client_traces.into_iter().next().unwrap();
            let (method, uri_actual) = assert_matches!(
                http_client_trace,
                HttpClientTrace {
                    req: RequestTrace {
                        method,
                        sent_at: _,
                        uri
                    },
                    resp: Some(ResponseTrace {
                        status: Ok(200),
                        finished_at: _
                    })
                }
                => (method, uri)
            );
            assert_eq!("GET", method);
            assert_eq!(uri, *uri_actual);
            drop(db_connection);
            drop(exec_task);
            db_pool.close().await.unwrap();
        }

        #[expect(clippy::too_many_lines)]
        #[tokio::test]
        async fn http_get_activity_trap_should_be_turned_into_finished_execution_error_permanent_failure()
         {
            use wiremock::{
                Mock, MockServer, ResponseTemplate,
                matchers::{method, path},
            };
            const STATUS: u16 = 418; // I'm a teapot causes trap
            test_utils::set_up();
            info!("All set up");
            let sim_clock = SimClock::default();
            let (_guard, db_pool) = Database::Memory.set_up().await;
            let engine =
                Engines::get_activity_engine(EngineConfig::on_demand_testing().await).unwrap();
            let (worker, _) = new_activity_worker(
                test_programs_http_get_activity_builder::TEST_PROGRAMS_HTTP_GET_ACTIVITY,
                engine,
                sim_clock.clone(),
                TokioSleep,
            );
            let exec_config = ExecConfig {
                batch_size: 1,
                lock_expiry: Duration::from_secs(1),
                tick_sleep: Duration::ZERO,
                component_id: ComponentId::dummy_activity(),
                task_limiter: None,
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
            let uri = format!("http://127.0.0.1:{port}/", port = server_address.port());
            let params = Params::from_json_values(vec![json!(uri.clone())]);
            let execution_id = ExecutionId::generate();
            let created_at = sim_clock.now();
            let db_connection = db_pool.connection();
            info!("Creating execution");
            let stopwatch = std::time::Instant::now();
            db_connection
                .create(CreateRequest {
                    created_at,
                    execution_id: execution_id.clone(),
                    ffqn: HTTP_GET_SUCCESSFUL_ACTIVITY,
                    params,
                    parent: None,
                    metadata: concepts::ExecutionMetadata::empty(),
                    scheduled_at: created_at,
                    retry_exp_backoff: Duration::ZERO,
                    max_retries: 0,
                    component_id: ComponentId::dummy_activity(),
                    scheduled_by: None,
                })
                .await
                .unwrap();

            let server = MockServer::builder().listener(listener).start().await;
            Mock::given(method("GET"))
                .and(path("/"))
                .respond_with(ResponseTemplate::new(STATUS).set_body_string(""))
                .expect(1)
                .mount(&server)
                .await;

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
            let exec_log = db_connection.get(&execution_id).await.unwrap();
            let stopwatch = stopwatch.elapsed();
            info!("Finished in {stopwatch:?}");
            let (res, http_client_traces) = assert_matches!(
                exec_log.last_event().event.clone(),
                ExecutionEventInner::Finished { result, http_client_traces: Some(http_client_traces) }
                => (result, http_client_traces));
            let res = res.unwrap_err();
            assert_matches!(
                res,
                FinishedExecutionError::PermanentFailure {
                    reason_inner: _,
                    reason_full: _,
                    kind: PermanentFailureKind::ActivityTrap,
                    detail: _
                }
            );

            assert_eq!(1, http_client_traces.len());
            let http_client_trace = http_client_traces.into_iter().next().unwrap();
            let (method, uri_actual) = assert_matches!(
                http_client_trace,
                HttpClientTrace {
                    req: RequestTrace {
                        method,
                        sent_at: _,
                        uri
                    },
                    resp: Some(ResponseTrace {
                        status: Ok(STATUS),
                        finished_at: _
                    })
                }
                => (method, uri)
            );
            assert_eq!("GET", method);
            assert_eq!(uri, *uri_actual);
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
                Mock, MockServer, ResponseTemplate,
                matchers::{method, path},
            };
            const BODY: &str = "ok";
            const RETRY_EXP_BACKOFF: Duration = Duration::from_millis(10);
            test_utils::set_up();
            let sim_clock = SimClock::default();
            let (_guard, db_pool) = Database::Memory.set_up().await;
            let engine =
                Engines::get_activity_engine(EngineConfig::on_demand_testing().await).unwrap();
            let (worker, _) = new_activity_worker(
                test_programs_http_get_activity_builder::TEST_PROGRAMS_HTTP_GET_ACTIVITY,
                engine,
                sim_clock.clone(),
                TokioSleep,
            );
            let exec_config = ExecConfig {
                batch_size: 1,
                lock_expiry: Duration::from_secs(1),
                tick_sleep: Duration::ZERO,
                component_id: ComponentId::dummy_activity(),
                task_limiter: None,
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
            let uri = format!("http://127.0.0.1:{port}/", port = server_address.port());
            let params = Params::from_json_values(vec![json!(uri.clone())]);
            let execution_id = ExecutionId::generate();
            let created_at = sim_clock.now();
            let db_connection = db_pool.connection();
            db_connection
                .create(CreateRequest {
                    created_at,
                    execution_id: execution_id.clone(),
                    ffqn: HTTP_GET_SUCCESSFUL_ACTIVITY,
                    params,
                    parent: None,
                    metadata: concepts::ExecutionMetadata::empty(),
                    scheduled_at: created_at,
                    retry_exp_backoff: RETRY_EXP_BACKOFF,
                    max_retries: 1,
                    component_id: ComponentId::dummy_activity(),
                    scheduled_by: None,
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
                // Expect error result to be interpreted as an temporary failure
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
                let exec_log = db_connection.get(&execution_id).await.unwrap();

                let (reason_full, reason_inner, detail, found_expires_at, http_client_traces) = assert_matches!(
                    &exec_log.last_event().event,
                    ExecutionEventInner::TemporarilyFailed {
                        backoff_expires_at,
                        reason_full,
                        reason_inner,
                        detail: Some(detail),
                        http_client_traces: Some(http_client_traces)
                    }
                    => (reason_full, reason_inner, detail, *backoff_expires_at, http_client_traces)
                );
                assert_eq!(sim_clock.now() + RETRY_EXP_BACKOFF, found_expires_at);
                assert_eq!("activity returned error", reason_inner.deref());
                assert_eq!("activity returned error", reason_full.deref());
                assert!(
                    detail.contains("wrong status code: 500"),
                    "Unexpected {detail}"
                );

                assert_eq!(1, http_client_traces.len());
                let http_client_trace = http_client_traces.iter().next().unwrap();
                let (method, uri_actual) = assert_matches!(
                    http_client_trace,
                    HttpClientTrace {
                        req: RequestTrace {
                            method,
                            sent_at: _,
                            uri
                        },
                        resp: Some(ResponseTrace {
                            status: Ok(500),
                            finished_at: _
                        })
                    }
                    => (method, uri)
                );
                assert_eq!("GET", method);
                assert_eq!(uri, *uri_actual);
                server.verify().await;
            }
            // Noop until the timeout expires
            assert_eq!(
                0,
                exec_task
                    .tick_test(sim_clock.now())
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
                    .tick_test(sim_clock.now())
                    .await
                    .unwrap()
                    .wait_for_tasks()
                    .await
                    .unwrap()
            );
            let exec_log = db_connection.get(&execution_id).await.unwrap();
            let res = assert_matches!(exec_log.last_event().event.clone(), ExecutionEventInner::Finished { result, .. } => result);
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
            let (ok, err) =
                assert_matches!(res.val_type(), Some(TypeWrapper::Result{ok, err}) => (ok, err));
            assert_eq!(Some(Box::new(TypeWrapper::String)), *ok);
            assert_eq!(Some(Box::new(TypeWrapper::String)), *err);
            drop(db_connection);
            drop(exec_task);
            db_pool.close().await.unwrap();
        }
    }
}
