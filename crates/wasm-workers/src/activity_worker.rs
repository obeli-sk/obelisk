use crate::{EngineConfig, WasmComponent, WasmFileError};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use concepts::prefixed_ulid::ConfigId;
use concepts::storage::{HistoryEvent, Version};
use concepts::{ExecutionId, FunctionFqn, StrVariant};
use concepts::{Params, SupportedFunctionResult};
use executor::worker::FatalError;
use executor::worker::{Worker, WorkerError};
use std::{fmt::Debug, sync::Arc};
use tracing::{debug, info, trace};
use utils::time::{now_tokio_instant, ClockFn};
use utils::wasm_tools::ExIm;
use wasmtime::{component::Val, Engine};
use wasmtime::{Store, UpdateDeadline};

type StoreCtx = utils::wasi_http::Ctx;

#[derive(Clone, Debug, Copy)]
pub enum RecycleInstancesSetting {
    Enable,
    Disable,
}
impl From<bool> for RecycleInstancesSetting {
    fn from(value: bool) -> Self {
        if value {
            Self::Enable
        } else {
            Self::Disable
        }
    }
}

type MaybeRecycledInstances =
    Option<Arc<std::sync::Mutex<Vec<(wasmtime::component::Instance, Store<StoreCtx>)>>>>;

impl RecycleInstancesSetting {
    pub(crate) fn instantiate(&self) -> MaybeRecycledInstances {
        match self {
            Self::Enable => Some(Arc::default()),
            Self::Disable => None,
        }
    }
}

#[must_use]
pub fn activity_engine(config: EngineConfig) -> Arc<Engine> {
    let mut wasmtime_config = wasmtime::Config::new();
    wasmtime_config.wasm_backtrace_details(wasmtime::WasmBacktraceDetails::Enable);
    wasmtime_config.wasm_component_model(true);
    wasmtime_config.async_support(true);
    wasmtime_config.allocation_strategy(config.allocation_strategy);
    wasmtime_config.epoch_interruption(true);
    Arc::new(Engine::new(&wasmtime_config).unwrap())
}

#[derive(Clone, Debug)]
pub struct ActivityConfig<C: ClockFn> {
    pub config_id: ConfigId,
    pub recycled_instances: RecycleInstancesSetting,
    pub clock_fn: C,
}

#[derive(Clone)]
pub struct ActivityWorker<C: ClockFn> {
    config: ActivityConfig<C>,
    engine: Arc<Engine>,
    linker: wasmtime::component::Linker<StoreCtx>,
    component: wasmtime::component::Component,
    recycled_instances: MaybeRecycledInstances,
    exim: ExIm,
}

impl<C: ClockFn> ActivityWorker<C> {
    #[tracing::instrument(skip_all, fields(wasm_path = %wasm_component.wasm_path, config_id = %config.config_id))]
    pub fn new_with_config(
        wasm_component: WasmComponent,
        config: ActivityConfig<C>,
        engine: Arc<Engine>,
    ) -> Result<Self, WasmFileError> {
        let mut linker = wasmtime::component::Linker::new(&engine);
        // Link
        wasmtime_wasi::bindings::Command::add_to_linker(&mut linker, |t| t).map_err(|err| {
            WasmFileError::LinkingError {
                file: wasm_component.wasm_path.clone(),
                reason: StrVariant::Static("cannot add wasi command"),
                err: err.into(),
            }
        })?;
        wasmtime_wasi_http::bindings::http::outgoing_handler::add_to_linker(&mut linker, |t| t)
            .map_err(|err| WasmFileError::LinkingError {
                file: wasm_component.wasm_path.clone(),
                reason: StrVariant::Static("cannot add http outgoing_handler"),
                err: err.into(),
            })?;
        wasmtime_wasi_http::bindings::http::types::add_to_linker(&mut linker, |t| t).map_err(
            |err| WasmFileError::LinkingError {
                file: wasm_component.wasm_path.clone(),
                reason: StrVariant::Static("cannot add http types"),
                err: err.into(),
            },
        )?;

        let recycled_instances = config.recycled_instances.instantiate();
        Ok(Self {
            config,
            engine,
            linker,
            component: wasm_component.component,
            recycled_instances,
            exim: wasm_component.exim,
        })
    }
}

#[async_trait]
impl<C: ClockFn + 'static> Worker for ActivityWorker<C> {
    fn supported_functions(&self) -> impl Iterator<Item = &FunctionFqn> {
        self.exim.exported_functions()
    }

    #[allow(clippy::too_many_lines)]
    async fn run(
        &self,
        _execution_id: ExecutionId,
        ffqn: FunctionFqn,
        params: Params,
        events: Vec<HistoryEvent>,
        version: Version,
        execution_deadline: DateTime<Utc>,
    ) -> Result<(SupportedFunctionResult, Version), WorkerError> {
        trace!("Params: {params:?}");
        assert!(events.is_empty());
        let instance_and_store = self
            .recycled_instances
            .as_ref()
            .and_then(|i| i.lock().unwrap().pop());
        let (instance, mut store) = if let Some((instance, store)) = instance_and_store {
            (instance, store)
        } else {
            let mut store = utils::wasi_http::store(&self.engine);
            match self
                .linker
                .instantiate_async(&mut store, &self.component)
                .await
            {
                Ok(instance) => (instance, store),
                Err(err) => {
                    let reason = err.to_string();
                    if reason.starts_with("maximum concurrent") {
                        return Err(WorkerError::LimitReached(reason, version));
                    }
                    return Err(WorkerError::IntermittentError {
                        reason: StrVariant::Static("cannot instantiate"),
                        err: err.into(),
                        version,
                    });
                }
            }
        };
        store.epoch_deadline_callback(|_store_ctx| Ok(UpdateDeadline::Yield(1)));
        let call_function = async move {
            let func = {
                let mut exports = instance.exports(&mut store);
                let mut exports_instance = exports.root();
                let mut exports_instance = exports_instance
                    .instance(&ffqn.ifc_fqn)
                    .expect("interface must be found");
                exports_instance
                    .func(&ffqn.function_name)
                    .expect("function must be found")
            };
            let params = match params.as_vals(func.params(&store)) {
                Ok(params) => params,
                Err(err) => {
                    return Err(WorkerError::FatalError(
                        FatalError::ParamsParsingError(err),
                        version,
                    ));
                }
            };
            let result_types = func.results(&mut store);
            let mut results = vec![Val::Bool(false); result_types.len()];
            if let Err(err) = func.call_async(&mut store, &params, &mut results).await {
                let err = err.into();
                return Err(WorkerError::IntermittentError {
                    reason: StrVariant::Arc(Arc::from(format!(
                        "wasm function call error: `{err}`"
                    ))),
                    err,
                    version,
                });
            }; // guest panic exits here
            let result = match SupportedFunctionResult::new(
                results.into_iter().zip(result_types.iter().cloned()),
            ) {
                Ok(result) => result,
                Err(err) => {
                    return Err(WorkerError::FatalError(
                        FatalError::ResultParsingError(err),
                        version,
                    ))
                }
            };
            if let Err(err) = func.post_return_async(&mut store).await {
                return Err(WorkerError::IntermittentError {
                    reason: StrVariant::Static("wasm post function call error"),
                    err: err.into(),
                    version,
                });
            }

            if let Some(recycled_instances) = &self.recycled_instances {
                recycled_instances.lock().unwrap().push((instance, store));
            }

            Ok((result, version))
        };
        let started_at = (self.config.clock_fn)();
        let deadline_duration = (execution_deadline - started_at)
            .to_std()
            .unwrap_or_default();
        let stopwatch = now_tokio_instant(); // Not using `clock_fn` here is ok, value is only used for log reporting.
        tokio::select! {
            res = call_function =>{

                if let Err(err) = &res {
                    info!(%err, duration = ?stopwatch.elapsed(), ?deadline_duration, %execution_deadline, "Finished with an error");
                } else {
                    debug!(duration = ?stopwatch.elapsed(), ?deadline_duration,  %execution_deadline, "Finished");
                }
                res
            },
            ()   = tokio::time::sleep(deadline_duration) => {
                debug!(duration = ?stopwatch.elapsed(), ?deadline_duration, %execution_deadline, now = %(self.config.clock_fn)(), "Timed out");
                Err(WorkerError::IntermittentTimeout)
            }
        }
    }
}

mod valuable {
    use super::ActivityWorker;
    use utils::time::ClockFn;

    static FIELDS: &[::valuable::NamedField<'static>] = &[::valuable::NamedField::new("config_id")];
    impl<C: ClockFn> ::valuable::Structable for ActivityWorker<C> {
        fn definition(&self) -> ::valuable::StructDef<'_> {
            ::valuable::StructDef::new_static("ActivityWorker", ::valuable::Fields::Named(FIELDS))
        }
    }
    impl<C: ClockFn> ::valuable::Valuable for ActivityWorker<C> {
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
pub(crate) mod tests {
    use super::*;
    use crate::EngineConfig;
    use assert_matches::assert_matches;
    use concepts::{
        storage::{CreateRequest, DbConnection, DbPool},
        ExecutionId, Params, SupportedFunctionResult,
    };
    use db_mem::inmemory_dao::DbTask;
    use executor::executor::{ExecConfig, ExecTask, ExecutorTaskHandle};
    use serde_json::json;
    use std::time::Duration;
    use test_utils::env_or_default;
    use utils::time::now;
    use val_json::{
        type_wrapper::TypeWrapper,
        wast_val::{WastVal, WastValWithType},
    };

    pub const FIBO_ACTIVITY_FFQN: FunctionFqn = FunctionFqn::new_static_tuple(
        test_programs_fibo_activity_builder::exports::testing::fibo::fibo::FIBO,
    ); // func(n: u8) -> u64;
    pub const FIBO_10_INPUT: u8 = 10;
    pub const FIBO_10_OUTPUT: u64 = 55;

    pub(crate) fn spawn_activity<DB: DbConnection + 'static, P: DbPool<DB> + 'static>(
        db_pool: P,
        wasm_path: &'static str,
        ffqn: FunctionFqn,
    ) -> ExecutorTaskHandle {
        let engine = activity_engine(EngineConfig::default());
        let worker = Arc::new(
            ActivityWorker::new_with_config(
                WasmComponent::new(StrVariant::Static(wasm_path), &engine).unwrap(),
                ActivityConfig {
                    config_id: ConfigId::generate(),
                    recycled_instances: RecycleInstancesSetting::Disable,
                    clock_fn: now,
                },
                engine,
            )
            .unwrap(),
        );
        let exec_config = ExecConfig {
            ffqns: vec![ffqn],
            batch_size: 1,
            lock_expiry: Duration::from_secs(1),
            tick_sleep: Duration::ZERO,
            clock_fn: now,
        };
        ExecTask::spawn_new(worker, exec_config, db_pool, None)
    }

    pub(crate) fn spawn_activity_fibo<DB: DbConnection + 'static, P: DbPool<DB> + 'static>(
        db_pool: P,
    ) -> ExecutorTaskHandle {
        spawn_activity(
            db_pool,
            test_programs_fibo_activity_builder::TEST_PROGRAMS_FIBO_ACTIVITY,
            FIBO_ACTIVITY_FFQN,
        )
    }

    #[tokio::test]
    async fn fibo_once() {
        test_utils::set_up();
        let mut db_task = DbTask::spawn_new(1);
        let db_pool = db_task.pool().unwrap();
        let db_connection = db_pool.connection();
        let exec_task = spawn_activity_fibo(db_pool);
        // Create an execution.
        let execution_id = ExecutionId::generate();
        let created_at = now();
        let params = Params::from_json_array(json!([FIBO_10_INPUT])).unwrap();
        db_connection
            .create(CreateRequest {
                created_at,
                execution_id,
                ffqn: FIBO_ACTIVITY_FFQN,
                params,
                parent: None,
                scheduled_at: None,
                retry_exp_backoff: Duration::ZERO,
                max_retries: 0,
            })
            .await
            .unwrap();
        // Check the result.
        let fibo = assert_matches!(db_connection.wait_for_finished_result(execution_id, None).await.unwrap(),
            Ok(SupportedFunctionResult::Infallible(WastValWithType {value: WastVal::U64(val), r#type: TypeWrapper::U64 })) => val);
        assert_eq!(FIBO_10_OUTPUT, fibo);
        drop(db_connection);
        exec_task.close().await;
        db_task.close().await;
    }

    #[tokio::test]
    async fn limit_reached() {
        const FIBO_INPUT: u8 = 10;
        const RECYCLE: bool = true;
        const LOCK_EXPIRY_MILLIS: u64 = 1100;
        const TASKS: u32 = 10;
        const MAX_INSTANCES: u32 = 1;

        test_utils::set_up();
        let fibo_input = env_or_default("FIBO_INPUT", FIBO_INPUT);
        let recycled_instances = env_or_default("RECYCLE", RECYCLE).into();
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

        let engine = activity_engine(EngineConfig {
            allocation_strategy: wasmtime::InstanceAllocationStrategy::Pooling(pool),
        });
        let fibo_worker = ActivityWorker::new_with_config(
            WasmComponent::new(
                StrVariant::Static(
                    test_programs_fibo_activity_builder::TEST_PROGRAMS_FIBO_ACTIVITY,
                ),
                &engine,
            )
            .unwrap(),
            ActivityConfig {
                config_id: ConfigId::generate(),
                recycled_instances,
                clock_fn: now,
            },
            engine,
        )
        .unwrap();
        let execution_deadline = now() + lock_expiry;
        // create executions
        let join_handles = (0..tasks)
            .map(|_| {
                let fibo_worker = fibo_worker.clone();
                tokio::spawn(async move {
                    fibo_worker
                        .run(
                            ExecutionId::generate(),
                            FIBO_ACTIVITY_FFQN,
                            Params::from_json_array(json!([fibo_input])).unwrap(),
                            Vec::new(),
                            Version::new(0),
                            execution_deadline,
                        )
                        .await
                })
            })
            .collect::<Vec<_>>();
        let mut limit_reached = 0;
        for jh in join_handles {
            if matches!(jh.await.unwrap(), Err(WorkerError::LimitReached(..))) {
                limit_reached += 1;
            }
        }
        assert!(limit_reached > 0, "Limit was not reached");
    }

    #[cfg(not(madsim))] // Requires madsim support in wasmtime
    pub mod wasmtime_nosim {
        use super::*;
        use tracing::info;

        const EPOCH_MILLIS: u64 = 10;

        pub const SLEEP_LOOP_ACTIVITY_FFQN: FunctionFqn = FunctionFqn::new_static_tuple(
            test_programs_sleep_activity_builder::exports::testing::sleep::sleep::SLEEP_LOOP,
        ); // sleep-loop: func(millis: u64, iterations: u32);
        pub const HTTP_GET_ACTIVITY_FFQN: FunctionFqn = FunctionFqn::new_static_tuple(
            test_programs_http_get_activity_builder::exports::testing::http::http_get::GET,
        );
        // get: func(authority: string, path: string) -> result<string, string>;

        #[rstest::rstest]
        #[case(10, 100, Err(concepts::FinishedExecutionError::PermanentTimeout))] // 1s -> timeout
        #[case(10, 10, Ok(SupportedFunctionResult::None))] // 0.1s -> Ok
        #[case(1500, 1, Err(concepts::FinishedExecutionError::PermanentTimeout))] // 1s -> timeout
        #[tokio::test]
        async fn sleep_should_produce_intermittent_timeout(
            #[case] sleep_millis: u32,
            #[case] sleep_iterations: u32,
            #[case] expected: concepts::FinishedExecutionResult,
            #[values(false, true)] recycle: bool,
        ) {
            const LOCK_EXPIRY: Duration = Duration::from_millis(500);
            const TICK_SLEEP: Duration = Duration::from_millis(10);
            test_utils::set_up();
            let mut db_task = DbTask::spawn_new(1);
            let db_pool = db_task.pool().expect("must be open");
            let timers_watcher_task =
                executor::expired_timers_watcher::TimersWatcherTask::spawn_new(
                    db_pool.connection(),
                    executor::expired_timers_watcher::TimersWatcherConfig {
                        tick_sleep: TICK_SLEEP,
                        clock_fn: now,
                    },
                )
                .unwrap();
            let engine = activity_engine(EngineConfig::default());
            let _epoch_ticker = crate::epoch_ticker::EpochTicker::spawn_new(
                vec![engine.weak()],
                Duration::from_millis(EPOCH_MILLIS),
            );

            let recycled_instances: RecycleInstancesSetting =
                env_or_default("RECYCLE", recycle).into();
            let worker = Arc::new(
                ActivityWorker::new_with_config(
                    WasmComponent::new(
                        StrVariant::Static(
                            test_programs_sleep_activity_builder::TEST_PROGRAMS_SLEEP_ACTIVITY,
                        ),
                        &engine,
                    )
                    .unwrap(),
                    ActivityConfig {
                        config_id: ConfigId::generate(),
                        recycled_instances,
                        clock_fn: now,
                    },
                    engine,
                )
                .unwrap(),
            );

            let exec_config = ExecConfig {
                ffqns: vec![SLEEP_LOOP_ACTIVITY_FFQN],
                batch_size: 1,
                lock_expiry: LOCK_EXPIRY,
                tick_sleep: TICK_SLEEP,
                clock_fn: now,
            };
            let exec_task = ExecTask::spawn_new(worker, exec_config, db_pool.clone(), None);

            // Create an execution.
            let stopwatch = std::time::Instant::now();
            let execution_id = ExecutionId::generate();
            info!("Testing {execution_id}");
            let created_at = now();
            let db_connection = db_pool.connection();
            db_connection
                .create(CreateRequest {
                    created_at,
                    execution_id,
                    ffqn: SLEEP_LOOP_ACTIVITY_FFQN,
                    params: Params::from_json_array(json!([sleep_millis, sleep_iterations]))
                        .unwrap(),
                    parent: None,
                    scheduled_at: None,
                    retry_exp_backoff: Duration::ZERO,
                    max_retries: 0,
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
            drop(db_pool);
            timers_watcher_task.close().await;
            exec_task.close().await;
            db_task.close().await;
        }

        #[rstest::rstest]
        #[case(1, 2000)] // many small sleeps
        #[case(2000, 1)] // one long sleep
        #[tokio::test]
        async fn long_running_execution_should_timeout(
            #[case] sleep_millis: u32,
            #[case] sleep_iterations: u32,
        ) {
            const TIMEOUT: Duration = Duration::from_millis(200);
            test_utils::set_up();

            let engine = activity_engine(EngineConfig::default());
            let _epoch_ticker = crate::epoch_ticker::EpochTicker::spawn_new(
                vec![engine.weak()],
                Duration::from_millis(EPOCH_MILLIS),
            );

            let worker = Arc::new(
                ActivityWorker::new_with_config(
                    WasmComponent::new(
                        StrVariant::Static(
                            test_programs_sleep_activity_builder::TEST_PROGRAMS_SLEEP_ACTIVITY,
                        ),
                        &engine,
                    )
                    .unwrap(),
                    ActivityConfig {
                        config_id: ConfigId::generate(),
                        recycled_instances: RecycleInstancesSetting::Disable,
                        clock_fn: now,
                    },
                    engine,
                )
                .unwrap(),
            );

            let executed_at = now();
            let err = worker
                .run(
                    ExecutionId::generate(),
                    SLEEP_LOOP_ACTIVITY_FFQN,
                    Params::from_json_array(json!([sleep_millis, sleep_iterations])).unwrap(),
                    Vec::new(),
                    Version::new(0),
                    executed_at + TIMEOUT,
                )
                .await
                .unwrap_err();
            assert_matches!(err, WorkerError::IntermittentTimeout);
        }

        #[tokio::test]
        async fn http_get() {
            use std::ops::Deref;
            use wiremock::{
                matchers::{method, path},
                Mock, MockServer, ResponseTemplate,
            };
            const BODY: &str = "ok";
            test_utils::set_up();
            let mut db_task = DbTask::spawn_new(1);
            let db_pool = db_task.pool().unwrap();

            let exec_task = spawn_activity(
                db_pool.clone(),
                test_programs_http_get_activity_builder::TEST_PROGRAMS_HTTP_GET_ACTIVITY,
                HTTP_GET_ACTIVITY_FFQN,
            );
            let server = MockServer::start().await;
            Mock::given(method("GET"))
                .and(path("/"))
                .respond_with(ResponseTemplate::new(200).set_body_string(BODY))
                .expect(1)
                .mount(&server)
                .await;
            debug!("started mock server on {}", server.address());
            let params = Params::from_json_array(json!([
                format!("127.0.0.1:{port}", port = server.address().port()),
                "/"
            ]))
            .unwrap();

            // Create an execution.
            let execution_id = ExecutionId::generate();
            let created_at = now();
            let db_connection = db_pool.connection();
            db_connection
                .create(CreateRequest {
                    created_at,
                    execution_id,
                    ffqn: HTTP_GET_ACTIVITY_FFQN,
                    params,
                    parent: None,
                    scheduled_at: None,
                    retry_exp_backoff: Duration::from_millis(10),
                    max_retries: 5, // retries enabled due to racy test
                })
                .await
                .unwrap();
            // Check the result.
            let res = assert_matches!(
                db_connection
                    .wait_for_finished_result(execution_id, Some(Duration::from_secs(1)))
                    .await
                    .unwrap(),
                Ok(res) => res
            );
            let wast_val = assert_matches!(res.fallible_ok(), Some(Some(wast_val)) => wast_val);
            let val = assert_matches!(wast_val, WastVal::String(val) => val);
            assert_eq!(BODY, val.deref());
            // check types
            let (ok, err) = assert_matches!(res, SupportedFunctionResult::Fallible(WastValWithType{value: _,
                r#type: TypeWrapper::Result{ok, err}}) => (ok, err));
            assert_eq!(Some(Box::new(TypeWrapper::String)), ok);
            assert_eq!(Some(Box::new(TypeWrapper::String)), err);
            drop(db_connection);
            drop(db_pool);
            exec_task.close().await;
            db_task.close().await;
        }
    }
}
