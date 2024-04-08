use crate::{EngineConfig, WasmComponent, WasmFileError};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use concepts::prefixed_ulid::ConfigId;
use concepts::storage::{HistoryEvent, Version};
use concepts::{ExecutionId, FunctionFqn, StrVariant};
use concepts::{Params, SupportedFunctionResult};
use executor::worker::FatalError;
use executor::worker::{Worker, WorkerError};
use std::collections::HashMap;
use std::{fmt::Debug, sync::Arc};
use tracing::{debug, trace};
use utils::time::{now_tokio_instant, ClockFn};
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
    exported_ffqns_to_results_len: HashMap<FunctionFqn, usize>,
    linker: wasmtime::component::Linker<StoreCtx>,
    component: wasmtime::component::Component,
    recycled_instances: MaybeRecycledInstances,
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
        wasmtime_wasi::command::add_to_linker(&mut linker).map_err(|err| {
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
        // Compile
        let component = wasmtime::component::Component::from_binary(&engine, &wasm_component.wasm)
            .map_err(|err| {
                WasmFileError::CompilationError(wasm_component.wasm_path.clone(), err.into())
            })?;
        let recycled_instances = config.recycled_instances.instantiate();
        Ok(Self {
            config,
            engine,
            exported_ffqns_to_results_len: wasm_component.exported_ffqns_to_results_len,
            linker,
            component,
            recycled_instances,
        })
    }
}

#[async_trait]
impl<C: ClockFn + 'static> Worker for ActivityWorker<C> {
    async fn run(
        &self,
        _execution_id: ExecutionId,
        ffqn: FunctionFqn,
        params: Params,
        events: Vec<HistoryEvent>,
        version: Version,
        execution_deadline: DateTime<Utc>,
    ) -> Result<(SupportedFunctionResult, Version), (WorkerError, Version)> {
        assert!(events.is_empty());
        self.run(ffqn, params, execution_deadline)
            .await
            .map(|supported_result| (supported_result, version))
            .map_err(|err| (err, version))
    }

    fn supported_functions(&self) -> impl Iterator<Item = &FunctionFqn> {
        self.exported_ffqns_to_results_len.keys()
    }
}

impl<C: ClockFn + 'static> ActivityWorker<C> {
    #[tracing::instrument(skip_all)]
    async fn run(
        &self,
        ffqn: FunctionFqn,
        params: Params,
        execution_deadline: DateTime<Utc>,
    ) -> Result<SupportedFunctionResult, WorkerError> {
        let results_len = *self
            .exported_ffqns_to_results_len
            .get(&ffqn)
            .expect("executor must only run existing functions");
        trace!("Params: {params:?}, results_len:{results_len}",);

        let instance_and_store = self
            .recycled_instances
            .as_ref()
            .and_then(|i| i.lock().unwrap().pop());
        let (instance, mut store) = if let Some((instance, store)) = instance_and_store {
            (instance, store)
        } else {
            let mut store = utils::wasi_http::store(&self.engine);
            let instance = self
                .linker
                .instantiate_async(&mut store, &self.component)
                .await
                .map_err(|err| {
                    let reason = err.to_string();
                    if reason.starts_with("maximum concurrent") {
                        WorkerError::LimitReached(reason)
                    } else {
                        WorkerError::IntermittentError {
                            reason: StrVariant::Static("cannot instantiate"),
                            err: err.into(),
                        }
                    }
                })?;
            (instance, store)
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
            let param_types = func.params(&store);
            let mut results = vec![Val::Bool(false); results_len];
            func.call_async(
                &mut store,
                &params
                    .as_vals(&param_types)
                    .map_err(|err| WorkerError::FatalError(FatalError::ParamsParsingError(err)))?,
                &mut results,
            )
            .await
            .map_err(|err| {
                let err = err.into();
                WorkerError::IntermittentError {
                    reason: StrVariant::Arc(Arc::from(format!(
                        "wasm function call error: `{err}`"
                    ))),
                    err,
                }
            })?; // guest panic exits here
            let result = SupportedFunctionResult::new(results)
                .map_err(|err| WorkerError::FatalError(FatalError::ResultParsingError(err)))?;
            func.post_return_async(&mut store).await.map_err(|err| {
                WorkerError::IntermittentError {
                    reason: StrVariant::Static("wasm post function call error"),
                    err: err.into(),
                }
            })?;

            if let Some(recycled_instances) = &self.recycled_instances {
                recycled_instances.lock().unwrap().push((instance, store));
            }

            Ok(result)
        };
        let started_at = (self.config.clock_fn)();
        let deadline_duration = (execution_deadline - started_at)
            .to_std()
            .unwrap_or_default();
        let stopwatch = now_tokio_instant(); // Not using `clock_fn` here is ok, value is only used for log reporting.
        tokio::select! {
            res = call_function =>{
                debug!(duration = ?stopwatch.elapsed(), ?deadline_duration,  "Finished");
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
    use concepts::{storage::DbConnection, ExecutionId, Params, SupportedFunctionResult};
    use db::inmemory_dao::DbTask;
    use executor::executor::{ExecConfig, ExecTask, ExecutorTaskHandle};
    use std::time::{Duration, Instant};
    use test_utils::env_or_default;
    use tracing::warn;
    use utils::time::now;
    use val_json::wast_val::WastVal;
    use wasmtime::component::Val;

    pub const EPOCH_MILLIS: u64 = 10;
    pub const FIBO_ACTIVITY_FFQN: FunctionFqn =
        FunctionFqn::new_static_tuple(test_programs_fibo_activity_builder::FIBO); // func(n: u8) -> u64;
    pub const FIBO_10_INPUT: u8 = 10;
    pub const FIBO_10_OUTPUT: u64 = 55;

    pub(crate) fn spawn_activity<DB: DbConnection>(
        db_connection: DB,
        wasm_path: &'static str,
        ffqn: FunctionFqn,
    ) -> ExecutorTaskHandle {
        let worker = Arc::new(
            ActivityWorker::new_with_config(
                WasmComponent::new(StrVariant::Static(wasm_path)).unwrap(),
                ActivityConfig {
                    config_id: ConfigId::generate(),
                    recycled_instances: RecycleInstancesSetting::Disable,
                    clock_fn: now,
                },
                activity_engine(EngineConfig::default()),
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
        ExecTask::spawn_new(db_connection, worker, exec_config, None)
    }

    pub(crate) fn spawn_activity_fibo<DB: DbConnection>(db_connection: DB) -> ExecutorTaskHandle {
        spawn_activity(
            db_connection,
            test_programs_fibo_activity_builder::TEST_PROGRAMS_FIBO_ACTIVITY,
            FIBO_ACTIVITY_FFQN,
        )
    }

    #[tokio::test]
    async fn fibo_once() {
        test_utils::set_up();
        let mut db_task = DbTask::spawn_new(1);
        let db_connection = db_task.as_db_connection().expect("must be open");
        let exec_task = spawn_activity_fibo(db_connection.clone());
        // Create an execution.
        let execution_id = ExecutionId::generate();
        let created_at = now();
        db_connection
            .create(
                created_at,
                execution_id,
                FIBO_ACTIVITY_FFQN,
                Params::from([Val::U8(FIBO_10_INPUT)]),
                None,
                None,
                Duration::ZERO,
                0,
            )
            .await
            .unwrap();
        // Check the result.
        let fibo = assert_matches!(db_connection.wait_for_finished_result(execution_id, None).await.unwrap(),
            Ok(SupportedFunctionResult::Infallible(WastVal::U64(val))) => val);
        assert_eq!(FIBO_10_OUTPUT, fibo);
        drop(db_connection);
        exec_task.close().await;
        db_task.close().await;
    }

    #[cfg(test)]
    #[allow(clippy::too_many_lines)]
    #[tokio::test(flavor = "multi_thread", worker_threads = 20)]
    async fn perf_fibo_parallel() {
        use concepts::storage::{AppendRequest, ExecutionEventInner};
        use std::sync::Arc;

        const EXECUTIONS: usize = 20_000; // release: 70_000
        const RECYCLE: bool = true;
        const PERMITS: usize = 1_000_000;
        const BATCH_SIZE: u32 = 10_000;
        const LOCK_EXPIRY_MILLIS: u64 = 1100;
        const TICK_SLEEP_MILLIS: u64 = 0;

        const TASKS: u32 = 1; // release: 6
        const MAX_INSTANCES: u32 = 10_000;
        const DB_RPC_CAPACITY: usize = 1;

        test_utils::set_up();
        let fibo_input = env_or_default("FIBO_INPUT", FIBO_10_INPUT);
        let executions = env_or_default("EXECUTIONS", EXECUTIONS);
        let recycled_instances = env_or_default("RECYCLE", RECYCLE).into();
        let permits = env_or_default("PERMITS", PERMITS);
        let batch_size = env_or_default("BATCH_SIZE", BATCH_SIZE);
        let lock_expiry =
            Duration::from_millis(env_or_default("LOCK_EXPIRY_MILLIS", LOCK_EXPIRY_MILLIS));
        let tick_sleep =
            Duration::from_millis(env_or_default("TICK_SLEEP_MILLIS", TICK_SLEEP_MILLIS));
        let tasks = env_or_default("TASKS", TASKS);
        let max_instances = env_or_default("MAX_INSTANCES", MAX_INSTANCES);
        let db_rpc_capacity = env_or_default("DB_RPC_CAPACITY", DB_RPC_CAPACITY);

        let mut pool = wasmtime::PoolingAllocationConfig::default();
        pool.total_component_instances(max_instances);
        pool.total_stacks(max_instances);
        pool.total_core_instances(max_instances);
        pool.total_memories(max_instances);
        pool.total_tables(max_instances);

        let engine = activity_engine(EngineConfig {
            allocation_strategy: wasmtime::InstanceAllocationStrategy::Pooling(pool),
        });
        // Start epoch ticking
        let _epoch_ticker = crate::epoch_ticker::EpochTicker::spawn_new(
            vec![engine.weak()],
            Duration::from_millis(EPOCH_MILLIS),
        );

        // Spawn db
        let mut db_task = DbTask::spawn_new(db_rpc_capacity);
        let db_connection = db_task.as_db_connection().expect("must be open");

        let fibo_worker = Arc::new(
            ActivityWorker::new_with_config(
                WasmComponent::new(StrVariant::Static(
                    test_programs_fibo_activity_builder::TEST_PROGRAMS_FIBO_ACTIVITY,
                ))
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

        // create executions
        let stopwatch = Instant::now();
        let fibo_ffqn = FIBO_ACTIVITY_FFQN;
        let params = Params::from([Val::U8(fibo_input)]);

        let created_at = now();
        let mut execution_ids = Vec::with_capacity(executions);
        for _ in 0..executions {
            let execution_id = ExecutionId::generate();
            let req = AppendRequest {
                created_at,
                event: ExecutionEventInner::Created {
                    ffqn: fibo_ffqn.clone(),
                    params: params.clone(),
                    parent: None,
                    scheduled_at: None,
                    retry_exp_backoff: Duration::ZERO,
                    max_retries: 0,
                },
            };
            db_connection
                .append_batch(vec![req], execution_id, None)
                .await
                .unwrap();
            execution_ids.push(execution_id);
        }

        warn!(
            "Created {executions} executions in {:?}",
            stopwatch.elapsed()
        );

        // spawn executors
        let stopwatch = Instant::now();
        let task_limiter = if permits == 0 {
            None
        } else {
            Some(Arc::new(tokio::sync::Semaphore::new(permits)))
        };
        let exec_tasks = (0..tasks)
            .map(|_| {
                let exec_config = ExecConfig {
                    ffqns: vec![fibo_ffqn.clone()],
                    batch_size,
                    lock_expiry,
                    tick_sleep,
                    clock_fn: now,
                };
                ExecTask::spawn_new(
                    db_task.as_db_connection().unwrap(),
                    fibo_worker.clone(),
                    exec_config.clone(),
                    task_limiter.clone(),
                )
            })
            .collect::<Vec<_>>();

        let mut counter = 0;
        for execution_id in execution_ids {
            // Check that the computation succeded.
            assert_matches!(
                db_connection
                    .wait_for_finished_result(execution_id, Some(Duration::from_secs(2)))
                    .await
                    .unwrap(),
                Ok(SupportedFunctionResult::Infallible(WastVal::U64(_)))
            );
            counter += 1;
        }
        warn!(
            "Finished {counter} in {} ms",
            stopwatch.elapsed().as_millis()
        );
        drop(db_connection);
        for exec_task in exec_tasks {
            exec_task.close().await;
        }
        db_task.close().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 20)]
    async fn perf_fibo_direct() {
        const FIBO_INPUT: u8 = 10;
        const EXECUTIONS: u32 = 400_000; // release: 800_000
        const RECYCLE: bool = true;
        const LOCK_EXPIRY_MILLIS: u64 = 1100;
        const TASKS: u32 = 10_000;
        const MAX_INSTANCES: u32 = 10_000;
        test_utils::set_up();
        let fibo_input = env_or_default("FIBO_INPUT", FIBO_INPUT);
        let executions = env_or_default("EXECUTIONS", EXECUTIONS);
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
            WasmComponent::new(StrVariant::Static(
                test_programs_fibo_activity_builder::TEST_PROGRAMS_FIBO_ACTIVITY,
            ))
            .unwrap(),
            ActivityConfig {
                config_id: ConfigId::generate(),
                recycled_instances,
                clock_fn: now,
            },
            engine,
        )
        .unwrap();

        let stopwatch = Instant::now();
        let execution_deadline = now() + lock_expiry;
        // create executions

        let join_handles = (0..tasks)
            .map(|_| {
                let fibo_worker = fibo_worker.clone();
                tokio::spawn(async move {
                    let mut vec = Vec::with_capacity(usize::try_from(executions / tasks).unwrap());
                    for _ in 0..executions / tasks {
                        vec.push(
                            fibo_worker
                                .run(
                                    FIBO_ACTIVITY_FFQN,
                                    Params::from([Val::U8(fibo_input)]),
                                    execution_deadline,
                                )
                                .await,
                        );
                    }
                    vec
                })
            })
            .collect::<Vec<_>>();

        let mut counter = 0;
        for jh in join_handles {
            for res in jh.await.unwrap() {
                counter += 1;
                // Check that the computation succeded.
                assert_matches!(
                    res,
                    Ok(SupportedFunctionResult::Infallible(WastVal::U64(_)))
                );
            }
        }
        warn!(
            "Finished {counter} in {} ms",
            stopwatch.elapsed().as_millis()
        );
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

        let fibo_worker = ActivityWorker::new_with_config(
            WasmComponent::new(StrVariant::Static(
                test_programs_fibo_activity_builder::TEST_PROGRAMS_FIBO_ACTIVITY,
            ))
            .unwrap(),
            ActivityConfig {
                config_id: ConfigId::generate(),
                recycled_instances,
                clock_fn: now,
            },
            activity_engine(EngineConfig {
                allocation_strategy: wasmtime::InstanceAllocationStrategy::Pooling(pool),
            }),
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
                            FIBO_ACTIVITY_FFQN,
                            Params::from([Val::U8(fibo_input)]),
                            execution_deadline,
                        )
                        .await
                })
            })
            .collect::<Vec<_>>();
        let mut limit_reached = 0;
        for jh in join_handles {
            if matches!(jh.await.unwrap(), Err(WorkerError::LimitReached(_))) {
                limit_reached += 1;
            }
        }
        assert!(limit_reached > 0, "Limit was not reached");
    }

    #[cfg(all(test, not(madsim)))] // Requires madsim support in wasmtime
    mod wasmtime_nosim {
        use super::*;
        pub const SLEEP_LOOP_ACTIVITY_FFQN: FunctionFqn =
            FunctionFqn::new_static_tuple(test_programs_sleep_activity_builder::SLEEP_LOOP); // sleep-loop: func(millis: u64, iterations: u32);

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
            test_utils::set_up();
            let mut db_task = DbTask::spawn_new(1);
            let db_connection = db_task.as_db_connection().expect("must be open");

            let engine = activity_engine(EngineConfig::default());
            let _epoch_ticker = crate::epoch_ticker::EpochTicker::spawn_new(
                vec![engine.weak()],
                Duration::from_millis(EPOCH_MILLIS),
            );

            let recycled_instances: RecycleInstancesSetting =
                env_or_default("RECYCLE", recycle).into();
            let worker = Arc::new(
                ActivityWorker::new_with_config(
                    WasmComponent::new(StrVariant::Static(
                        test_programs_sleep_activity_builder::TEST_PROGRAMS_SLEEP_ACTIVITY,
                    ))
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
                tick_sleep: Duration::from_millis(10),
                clock_fn: now,
            };
            let exec_task = ExecTask::spawn_new(
                db_task.as_db_connection().unwrap(),
                worker,
                exec_config,
                None,
            );

            // Create an execution.
            let stopwatch = Instant::now();
            let execution_id = ExecutionId::generate();
            warn!("Testing {execution_id}");
            let created_at = now();
            db_connection
                .create(
                    created_at,
                    execution_id,
                    SLEEP_LOOP_ACTIVITY_FFQN,
                    Params::from([Val::U32(sleep_millis), Val::U32(sleep_iterations)]),
                    None,
                    None,
                    Duration::ZERO,
                    0,
                )
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
            warn!("Finished in {stopwatch:?}");
            assert!(stopwatch < LOCK_EXPIRY * 2);

            drop(db_connection);
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
                    WasmComponent::new(StrVariant::Static(
                        test_programs_sleep_activity_builder::TEST_PROGRAMS_SLEEP_ACTIVITY,
                    ))
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
                    SLEEP_LOOP_ACTIVITY_FFQN,
                    Params::from([Val::U32(sleep_millis), Val::U32(sleep_iterations)]),
                    executed_at + TIMEOUT,
                )
                .await
                .unwrap_err();
            assert_matches!(err, WorkerError::IntermittentTimeout);
        }

        const HTTP_GET_ACTIVITY_FFQN: FunctionFqn =
            FunctionFqn::new_static_tuple(test_programs_http_get_activity_builder::GET);
        // get: func(authority: string, path: string) -> result<string, string>;

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
            let db_connection = db_task.as_db_connection().expect("must be open");

            let exec_task = spawn_activity(
                db_connection.clone(),
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
            let params = Params::from([
                Val::String(format!("127.0.0.1:{port}", port = server.address().port()).into()),
                Val::String("/".into()),
            ]);
            // Create an execution.
            let execution_id = ExecutionId::generate();
            let created_at = now();
            db_connection
                .create(
                    created_at,
                    execution_id,
                    HTTP_GET_ACTIVITY_FFQN,
                    params,
                    None,
                    None,
                    Duration::from_millis(10),
                    5, // retries enabled due to racy test
                )
                .await
                .unwrap();
            // Check the result.
            let (val, res) = assert_matches!(
                db_connection.wait_for_finished_result(execution_id, Some(Duration::from_secs(1))).await.unwrap(),
                Ok(SupportedFunctionResult::Fallible(val, res)) => (val, res));
            res.unwrap();
            let val = assert_matches!(val, WastVal::Result(Ok(Some(val))) => val);
            let val = assert_matches!(val.deref(), WastVal::String(val) => val);
            assert_eq!(BODY, val.deref());
            drop(db_connection);
            exec_task.close().await;
            db_task.close().await;
        }
    }
}
