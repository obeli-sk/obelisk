use crate::EngineConfig;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use concepts::prefixed_ulid::{ActivityId, ConfigId};
use concepts::FunctionFqn;
use concepts::{Params, SupportedFunctionResult};
use derivative::Derivative;
use scheduler::worker::FatalError;
use scheduler::{
    storage::{HistoryEvent, Version},
    worker::{Worker, WorkerError},
};
use std::collections::HashMap;
use std::{borrow::Cow, error::Error, fmt::Debug, sync::Arc};
use tracing::{debug, info, trace};
use tracing_unwrap::{OptionExt, ResultExt};
use utils::time::{now, now_tokio_instant};
use utils::wasm_tools;
use wasmtime::{component::Val, Engine};
use wasmtime::{Store, UpdateDeadline};

pub fn activity_engine(config: EngineConfig) -> Arc<Engine> {
    let mut wasmtime_config = wasmtime::Config::new();
    wasmtime_config.wasm_backtrace_details(wasmtime::WasmBacktraceDetails::Enable);
    wasmtime_config.wasm_component_model(true);
    wasmtime_config.async_support(true);
    wasmtime_config.allocation_strategy(config.allocation_strategy);
    wasmtime_config.epoch_interruption(true);
    Arc::new(Engine::new(&wasmtime_config).unwrap_or_log())
}

#[derive(Clone, Derivative)]
#[derivative(Debug)]
#[allow(clippy::module_name_repetitions)]
pub struct ActivityConfig {
    pub config_id: ConfigId,
    pub wasm_path: Cow<'static, str>,
    pub epoch_millis: u64,
    #[derivative(Debug = "ignore")]
    pub recycled_instances: MaybeRecycledInstances,
}

type MaybeRecycledInstances = Option<
    Arc<std::sync::Mutex<Vec<(wasmtime::component::Instance, Store<utils::wasi_http::Ctx>)>>>,
>;

#[derive(Clone)]
pub struct ActivityWorker {
    config: ActivityConfig,
    engine: Arc<Engine>,
    ffqns_to_results_len: HashMap<FunctionFqn, usize>,
    linker: wasmtime::component::Linker<utils::wasi_http::Ctx>,
    component: wasmtime::component::Component,
}

#[derive(thiserror::Error, Debug)]
pub enum ActivityError {
    #[error("cannot open `{0}` - {1}")]
    CannotOpen(Cow<'static, str>, std::io::Error),
    #[error("cannot decode `{0}` - {1}")]
    DecodeError(Cow<'static, str>, wasm_tools::DecodeError),
    #[error("cannot decode metadata `{0}` - {1}")]
    FunctionMetadataError(Cow<'static, str>, wasm_tools::FunctionMetadataError),
    #[error("cannot instantiate `{0}` - {1}")]
    InstantiationError(Cow<'static, str>, Box<dyn Error>),
}

impl ActivityWorker {
    #[tracing::instrument(skip_all, fields(wasm_path = %config.wasm_path, config_id = %config.config_id))]
    pub fn new_with_config(
        config: ActivityConfig,
        engine: Arc<Engine>,
    ) -> Result<Self, ActivityError> {
        info!("Reading");
        let wasm = std::fs::read(config.wasm_path.as_ref())
            .map_err(|err| ActivityError::CannotOpen(config.wasm_path.clone(), err))?;
        let (resolve, world_id) = wasm_tools::decode(&wasm)
            .map_err(|err| ActivityError::DecodeError(config.wasm_path.clone(), err))?;
        let exported_interfaces = wasm_tools::exported_ifc_fns(&resolve, &world_id)
            .map_err(|err| ActivityError::DecodeError(config.wasm_path.clone(), err))?;
        let ffqns_to_results_len = wasm_tools::functions_and_result_lengths(exported_interfaces)
            .map_err(|err| ActivityError::FunctionMetadataError(config.wasm_path.clone(), err))?;

        debug!(?ffqns_to_results_len, "Decoded functions");
        let mut linker = wasmtime::component::Linker::new(&engine);

        wasmtime_wasi::preview2::command::add_to_linker(&mut linker).map_err(|err| {
            ActivityError::InstantiationError(config.wasm_path.clone(), err.into())
        })?;
        wasmtime_wasi_http::bindings::http::outgoing_handler::add_to_linker(&mut linker, |t| t)
            .map_err(|err| {
                ActivityError::InstantiationError(config.wasm_path.clone(), err.into())
            })?;
        wasmtime_wasi_http::bindings::http::types::add_to_linker(&mut linker, |t| t).map_err(
            |err| ActivityError::InstantiationError(config.wasm_path.clone(), err.into()),
        )?;
        // Compile the wasm component
        let component =
            wasmtime::component::Component::from_binary(&engine, &wasm).map_err(|err| {
                ActivityError::InstantiationError(config.wasm_path.clone(), err.into())
            })?;

        Ok(Self {
            config,
            engine,
            ffqns_to_results_len,
            linker,
            component,
        })
    }
}

#[async_trait]
impl Worker<ActivityId> for ActivityWorker {
    async fn run(
        &self,
        _execution_id: ActivityId,
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
}

impl ActivityWorker {
    #[tracing::instrument(skip_all)]
    pub(crate) async fn run(
        &self,
        ffqn: FunctionFqn,
        params: Params,
        execution_deadline: DateTime<Utc>,
    ) -> Result<SupportedFunctionResult, WorkerError> {
        let results_len = *self
            .ffqns_to_results_len
            .get(&ffqn)
            .ok_or(WorkerError::FatalError(FatalError::NotFound))?;
        trace!("Params: {params:?}, results_len:{results_len}",);

        let instance_and_store = self
            .config
            .recycled_instances
            .as_ref()
            .and_then(|i| i.lock().unwrap_or_log().pop());
        let (instance, mut store) = match instance_and_store {
            Some((instance, store)) => (instance, store),
            None => {
                let mut store = utils::wasi_http::store(&self.engine);
                let instance = self
                    .linker
                    .instantiate_async(&mut store, &self.component)
                    .await
                    .map_err(|err| WorkerError::IntermittentError {
                        reason: Cow::Borrowed("cannot instantiate"),
                        err: err.into(),
                    })?;
                (instance, store)
            }
        };
        let epoch_millis = self.config.epoch_millis;
        store.epoch_deadline_callback(move |_store_ctx| {
            let delta = execution_deadline - now();
            match delta.to_std() {
                Err(_) => {
                    trace!(%execution_deadline, %delta, "Sending OutOfFuel");
                    Err(wasmtime::Trap::OutOfFuel.into())
                }
                Ok(duration) => {
                    let ticks = u64::try_from(duration.as_millis()).unwrap() / epoch_millis;
                    trace!("epoch_deadline_callback in {ticks}");
                    Ok(UpdateDeadline::Yield(ticks))
                }
            }
        });
        let call_function = async move {
            let func = {
                let mut exports = instance.exports(&mut store);
                let mut exports_instance = exports.root();
                let mut exports_instance = exports_instance
                    .instance(&ffqn.ifc_fqn)
                    .expect_or_log("interface must be found");
                exports_instance
                    .func(&ffqn.function_name)
                    .expect_or_log("function must be found")
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
                if matches!(err.downcast_ref(), Some(wasmtime::Trap::OutOfFuel)) {
                    WorkerError::IntermittentTimeout
                } else {
                    let err = err.into();
                    WorkerError::IntermittentError {
                        // FIXME: this might be a fatal error, e.g. params mismatch
                        reason: Cow::Owned(format!("wasm function call error: `{err}`")),
                        err,
                    }
                }
            })?; // guest panic exits here
            let result = SupportedFunctionResult::new(results)
                .map_err(|err| WorkerError::FatalError(FatalError::ResultParsingError(err)))?;
            func.post_return_async(&mut store).await.map_err(|err| {
                WorkerError::IntermittentError {
                    reason: Cow::Borrowed("wasm post function call error"),
                    err: err.into(),
                }
            })?;

            if let Some(recycled_instances) = &self.config.recycled_instances {
                recycled_instances
                    .lock()
                    .unwrap_or_log()
                    .push((instance, store));
            }

            Ok(result)
        };
        let deadline_duration = (execution_deadline - now()).to_std().unwrap_or_default();
        let stopwatch = now_tokio_instant();
        let sleep_until = stopwatch + deadline_duration;
        tokio::select! {
            res = call_function =>{
                debug!(duration = ?stopwatch.elapsed(), ?deadline_duration, %execution_deadline, res = ?res.as_ref().map(|_|()), "Finished");
                res
            },
            _   = tokio::time::sleep_until(sleep_until) => {
                debug!(duration = ?stopwatch.elapsed(), ?deadline_duration, %execution_deadline, now = %now(), "Timed out");
                Err(WorkerError::IntermittentTimeout) // Epoch interruption only applies to actively executing wasm.
            }
        }
    }
}

mod valuable {
    use super::*;
    static FIELDS: &[::valuable::NamedField<'static>] = &[::valuable::NamedField::new("config_id")];
    impl ::valuable::Structable for ActivityWorker {
        fn definition(&self) -> ::valuable::StructDef<'_> {
            ::valuable::StructDef::new_static("ActivityWorker", ::valuable::Fields::Named(FIELDS))
        }
    }
    impl ::valuable::Valuable for ActivityWorker {
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

#[cfg(all(test, not(madsim)))]
pub(crate) mod tests {
    use super::*;
    use crate::EngineConfig;
    use assert_matches::assert_matches;
    use concepts::{
        prefixed_ulid::ActivityId, ExecutionId, FunctionFqnStr, Params, SupportedFunctionResult,
    };
    use rstest::rstest;
    use scheduler::{
        executor::{ExecConfig, ExecTask, ExecutorTaskHandle},
        storage::{inmemory_dao::DbTask, DbConnection, ExecutionIdStr},
        FinishedExecutionError, FinishedExecutionResult,
    };
    use std::{
        borrow::Cow,
        time::{Duration, Instant},
    };
    use tracing::{info, warn};
    use tracing_unwrap::{OptionExt, ResultExt};
    use utils::time::now;
    use val_json::wast_val::WastVal;
    use wasmtime::component::Val;

    pub const FIBO_FFQN: FunctionFqnStr = FunctionFqnStr::new("testing:fibo/fibo", "fibo"); // func(n: u8) -> u64;

    pub(crate) fn spawn_fibo_executor<DB: DbConnection>(db_connection: DB) -> ExecutorTaskHandle {
        let fibo_worker = ActivityWorker::new_with_config(
            ActivityConfig {
                wasm_path: Cow::Borrowed(
                    test_programs_fibo_activity_builder::TEST_PROGRAMS_FIBO_ACTIVITY,
                ),
                epoch_millis: 10,
                config_id: ConfigId::generate(),
                recycled_instances: None,
            },
            activity_engine(EngineConfig::default()),
        )
        .unwrap_or_log();

        let exec_config = ExecConfig {
            ffqns: vec![FIBO_FFQN.to_owned()],
            batch_size: 1,
            lock_expiry: Duration::from_secs(1),
            lock_expiry_leeway: Duration::from_millis(10),
            tick_sleep: Duration::from_millis(100),
        };
        ExecTask::spawn_new(db_connection, fibo_worker, exec_config.clone(), None)
    }

    #[tokio::test]
    async fn fibo_once() {
        const FIBO_INPUT: u8 = 10;
        const EXPECTED: u64 = 89;
        test_utils::set_up();
        let mut db_task = DbTask::spawn_new(1);
        let db_connection = db_task.as_db_connection().expect_or_log("must be open");
        let exec_task = spawn_fibo_executor(db_connection.clone());
        // Create an execution.
        let execution_id: ExecutionIdStr = ActivityId::generate().into();
        let created_at = now();
        db_connection
            .create(
                created_at,
                execution_id.clone(),
                FIBO_FFQN.to_owned(),
                Params::from([Val::U8(FIBO_INPUT)]),
                None,
                None,
                Duration::ZERO,
                0,
            )
            .await
            .unwrap_or_log();
        // Check the result.
        let fibo = assert_matches!(db_connection.obtain_finished_result(execution_id).await.unwrap_or_log(),
            Ok(SupportedFunctionResult::Infallible(WastVal::U64(val))) => val);
        assert_eq!(EXPECTED, fibo);
        drop(db_connection);
        exec_task.close().await;
        db_task.close().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 20)]
    async fn perf_fibo_parallel() {
        use scheduler::storage::{AppendRequest, ExecutionEventInner};
        use std::sync::Arc;
        use test_utils::env_or_default;

        const FIBO_INPUT: u8 = 10;
        const EXECUTIONS: usize = 20_000; // release: 70_000
        const RECYCLE: bool = true;
        const PERMITS: usize = 1_000_000;
        const BATCH_SIZE: u32 = 10_000;
        const LOCK_EXPIRY_MILLIS: u64 = 1100;
        const TICK_SLEEP_MILLIS: u64 = 0;
        const EPOCH_MILLIS: u64 = 10;
        const TASKS: u32 = 1; // release: 6
        const MAX_INSTANCES: u32 = 10_000;
        const DB_RPC_CAPACITY: usize = 1;

        test_utils::set_up();
        let fibo_input = env_or_default("FIBO_INPUT", FIBO_INPUT);
        let executions = env_or_default("EXECUTIONS", EXECUTIONS);
        let recycled_instances = env_or_default("RECYCLE", RECYCLE).then(Default::default);
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
        {
            let engine = engine.clone();
            tokio::spawn(async move {
                loop {
                    tokio::time::sleep(Duration::from_millis(EPOCH_MILLIS)).await;
                    engine.increment_epoch();
                }
            });
        }

        // Spawn db
        let mut db_task = DbTask::spawn_new(db_rpc_capacity);
        let db_connection = db_task.as_db_connection().expect_or_log("must be open");

        let fibo_worker = ActivityWorker::new_with_config(
            ActivityConfig {
                config_id: ConfigId::generate(),
                wasm_path: Cow::Borrowed(
                    test_programs_fibo_activity_builder::TEST_PROGRAMS_FIBO_ACTIVITY,
                ),
                epoch_millis: EPOCH_MILLIS,
                recycled_instances,
            },
            engine,
        )
        .unwrap_or_log();

        // create executions
        let stopwatch = Instant::now();
        let fibo_ffqn = FIBO_FFQN.to_owned();
        let params = Params::from([Val::U8(fibo_input)]);
        let (append, execution_ids): (_, Vec<_>) = {
            let now = now();
            (0..executions)
                .map(|_| {
                    let execution_id: ExecutionIdStr = ActivityId::generate().into();
                    (
                        AppendRequest {
                            created_at: now,
                            execution_id: execution_id.clone(),
                            version: Version::default(),
                            event: ExecutionEventInner::Created {
                                ffqn: fibo_ffqn.clone(),
                                params: params.clone(),
                                parent: None,
                                scheduled_at: None,
                                retry_exp_backoff: Duration::ZERO,
                                max_retries: 0,
                            },
                        },
                        execution_id,
                    )
                })
                .unzip()
        };
        db_connection.append_batch(append).await.unwrap();
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
                    lock_expiry_leeway: Duration::from_millis(10),
                    tick_sleep,
                };
                ExecTask::spawn_new(
                    db_task.as_db_connection().unwrap_or_log(),
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
                    .obtain_finished_result(execution_id)
                    .await
                    .unwrap_or_log(),
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
        use test_utils::env_or_default;

        const FIBO_INPUT: u8 = 10;
        const EXECUTIONS: u32 = 400_000; // release: 800_000
        const RECYCLE: bool = true;
        const LOCK_EXPIRY_MILLIS: u64 = 1100;
        const TASKS: u32 = 10_000;
        const MAX_INSTANCES: u32 = 10_000;

        test_utils::set_up();
        let fibo_input = env_or_default("FIBO_INPUT", FIBO_INPUT);
        let executions = env_or_default("EXECUTIONS", EXECUTIONS);
        let recycled_instances = env_or_default("RECYCLE", RECYCLE).then(Default::default);
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
            ActivityConfig {
                config_id: ConfigId::generate(),
                wasm_path: Cow::Borrowed(
                    test_programs_fibo_activity_builder::TEST_PROGRAMS_FIBO_ACTIVITY,
                ),
                epoch_millis: 10,
                recycled_instances,
            },
            activity_engine(EngineConfig {
                allocation_strategy: wasmtime::InstanceAllocationStrategy::Pooling(pool),
            }),
        )
        .unwrap_or_log();

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
                                    FIBO_FFQN.to_owned(),
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

    const SLEEP_FFQN: FunctionFqnStr = FunctionFqnStr::new("testing:sleep/sleep", "sleep-loop"); // func(millis: u64, iterations: u32);
    #[rstest]
    #[case(10, 100, Err(FinishedExecutionError::PermanentTimeout))] // 1s -> timeout
    #[case(10, 10, Ok(SupportedFunctionResult::None))] // 0.1s -> Ok
    #[case(1500, 1, Err(FinishedExecutionError::PermanentTimeout))] // 1s -> timeout
    #[tokio::test]
    async fn sleep_should_produce_intermittent_timeout(
        #[case] sleep_millis: u64,
        #[case] sleep_iterations: u32,
        #[case] expected: FinishedExecutionResult,
        #[values(false, true)] recycle: bool,
    ) {
        const EPOCH_MILLIS: u64 = 10;
        const LOCK_EXPIRY: Duration = Duration::from_millis(500);
        test_utils::set_up();
        let mut db_task = DbTask::spawn_new(1);
        let db_connection = db_task.as_db_connection().expect_or_log("must be open");

        let engine = activity_engine(EngineConfig::default());

        {
            let engine = engine.clone();
            tokio::spawn(async move {
                loop {
                    tokio::time::sleep(Duration::from_millis(EPOCH_MILLIS)).await;
                    engine.increment_epoch();
                }
            });
        }
        let recycled_instances = if recycle {
            Some(Default::default())
        } else {
            None
        };
        let fibo_worker = ActivityWorker::new_with_config(
            ActivityConfig {
                wasm_path: Cow::Borrowed(
                    test_programs_sleep_activity_builder::TEST_PROGRAMS_SLEEP_ACTIVITY,
                ),
                epoch_millis: EPOCH_MILLIS,
                config_id: ConfigId::generate(),
                recycled_instances: recycled_instances.clone(),
            },
            engine,
        )
        .unwrap_or_log();

        let exec_config = ExecConfig {
            ffqns: vec![SLEEP_FFQN.to_owned()],
            batch_size: 1,
            lock_expiry: LOCK_EXPIRY,
            lock_expiry_leeway: Duration::from_millis(10),
            tick_sleep: Duration::from_millis(10),
        };
        let exec_task = ExecTask::spawn_new(
            db_task.as_db_connection().unwrap_or_log(),
            fibo_worker,
            exec_config.clone(),
            None,
        );

        // Create an execution.
        let stopwatch = Instant::now();
        let execution_id: ExecutionIdStr = ActivityId::generate().into();
        info!("Testing {execution_id}");
        let created_at = now();
        db_connection
            .create(
                created_at,
                execution_id.clone(),
                SLEEP_FFQN.to_owned(),
                Params::from([Val::U64(sleep_millis), Val::U32(sleep_iterations)]),
                None,
                None,
                Duration::ZERO,
                0,
            )
            .await
            .unwrap_or_log();
        // Check the result.
        assert_matches!(
            db_connection
                .obtain_finished_result(execution_id)
                .await
                .unwrap_or_log(),
            expected
        );
        if recycle {
            assert_eq!(
                Some(if expected.is_ok() { 1 } else { 0 }),
                recycled_instances.as_ref().map(|i| i.lock().unwrap().len())
            );
        }
        let stopwatch = stopwatch.elapsed();
        warn!("Finished in {stopwatch:?}");
        assert!(stopwatch < LOCK_EXPIRY * 2);

        drop(db_connection);
        exec_task.close().await;
        db_task.close().await;
    }
}
