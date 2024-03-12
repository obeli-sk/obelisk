use async_trait::async_trait;
use chrono::{DateTime, Utc};
use concepts::prefixed_ulid::ActivityId;
use concepts::{FunctionFqn, FunctionMetadata};
use concepts::{Params, SupportedFunctionResult};
use scheduler::worker::FatalError;
use scheduler::{
    storage::{DbConnection, HistoryEvent, Version},
    worker::{Worker, WorkerError},
};
use std::{borrow::Cow, collections::HashMap, error::Error, fmt::Debug, sync::Arc};
use tracing::{debug, enabled, info, trace, Level};
use tracing_unwrap::{OptionExt, ResultExt};
use utils::time::now;
use utils::wasm_tools;
use wasmtime::{component::Val, Engine};
use wasmtime::{Store, UpdateDeadline};

#[derive(Debug, PartialEq, Eq, Clone)]
#[allow(clippy::module_name_repetitions)]
pub struct ActivityConfig {
    pub wasm_path: Cow<'static, str>,
    pub preload: bool,
    pub epoch_millis: u64,
}

type MaybeRecycledInstances = Option<
    Arc<std::sync::Mutex<Vec<(wasmtime::component::Instance, Store<utils::wasi_http::Ctx>)>>>,
>;

#[derive(derive_more::Display, Clone)]
#[display(fmt = "ActivityWorker")]
struct ActivityWorker {
    config: ActivityConfig,
    engine: Arc<Engine>,
    functions_to_metadata: HashMap<FunctionFqn, FunctionMetadata>,
    linker: wasmtime::component::Linker<utils::wasi_http::Ctx>,
    component: wasmtime::component::Component,
    instance_pre: Option<wasmtime::component::InstancePre<utils::wasi_http::Ctx>>,
    recycled_instances: MaybeRecycledInstances,
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
    #[tracing::instrument(skip_all, fields(wasm_path))]
    pub fn new_with_config<DB: DbConnection<ActivityId>>(
        _db_connection: DB,
        config: ActivityConfig,
        engine: Arc<Engine>,
        recycled_instances: MaybeRecycledInstances,
    ) -> Result<Self, ActivityError> {
        info!("Reading");
        let wasm = std::fs::read(config.wasm_path.as_ref())
            .map_err(|err| ActivityError::CannotOpen(config.wasm_path.clone(), err))?;
        let (resolve, world_id) = wasm_tools::decode(&wasm)
            .map_err(|err| ActivityError::DecodeError(config.wasm_path.clone(), err))?;
        let exported_interfaces = wasm_tools::exported_ifc_fns(&resolve, &world_id)
            .map_err(|err| ActivityError::DecodeError(config.wasm_path.clone(), err))?;
        let functions_to_metadata = wasm_tools::functions_to_metadata(exported_interfaces)
            .map_err(|err| ActivityError::FunctionMetadataError(config.wasm_path.clone(), err))?;
        if enabled!(Level::TRACE) {
            trace!(ffqns = ?functions_to_metadata.keys(), "Decoded functions {functions_to_metadata:#?}");
        } else {
            debug!(ffqns = ?functions_to_metadata.keys(), "Decoded functions" );
        }
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
        let instance_pre = if config.preload {
            Some(linker.instantiate_pre(&component).map_err(|err| {
                ActivityError::InstantiationError(config.wasm_path.clone(), err.into())
            })?)
        } else {
            None
        };

        Ok(Self {
            config,
            engine,
            functions_to_metadata,
            linker,
            component,
            instance_pre,
            recycled_instances,
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
        events: Vec<HistoryEvent<ActivityId>>,
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
        let results_len = self
            .functions_to_metadata
            .get(&ffqn)
            .ok_or(WorkerError::FatalError(FatalError::NotFound))?
            .results_len;
        trace!("Params: {params:?}, results_len:{results_len}",);

        let instance = self
            .recycled_instances
            .as_ref()
            .and_then(|i| i.lock().unwrap_or_log().pop());
        let (instance, mut store) = match (instance, &self.instance_pre) {
            (Some((instance, store)), _) => {
                trace!("Reusing old instance and store");
                (instance, store)
            }
            (None, Some(instance_pre)) => {
                let mut store = utils::wasi_http::store(&self.engine);
                let instance = instance_pre
                    .instantiate_async(&mut store)
                    .await
                    .map_err(|err| WorkerError::IntermittentError {
                        reason: Cow::Borrowed("cannot instantiate"),
                        err: err.into(),
                    })?;
                (instance, store)
            }
            (None, None) => {
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
            match (execution_deadline - now()).to_std() {
                Err(_) => {
                    trace!("Sending OutOfFuel");
                    Err(wasmtime::Trap::OutOfFuel.into())
                }
                Ok(duration) => {
                    let ticks = u64::try_from(duration.as_millis()).unwrap() / epoch_millis;
                    trace!("epoch_deadline_callback in {ticks}");
                    Ok(UpdateDeadline::Yield(ticks))
                }
            }
        });
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
        // call func
        let mut results = std::iter::repeat(Val::Bool(false))
            .take(results_len)
            .collect::<Vec<_>>();
        func.call_async(&mut store, &params, &mut results)
            .await
            .map_err(|err| {
                if matches!(err.downcast_ref(), Some(wasmtime::Trap::OutOfFuel)) {
                    WorkerError::IntermittentTimeout
                } else {
                    let err = err.into();
                    WorkerError::IntermittentError {
                        reason: Cow::Owned(format!("wasm function call error: `{err}`")),
                        err,
                    }
                }
            })?; // guest panic exits here
        let results = SupportedFunctionResult::new(results);
        func.post_return_async(&mut store)
            .await
            .map_err(|err| WorkerError::IntermittentError {
                reason: Cow::Borrowed("wasm post function call error"),
                err: err.into(),
            })?;

        if let Some(recycled_instances) = &self.recycled_instances {
            recycled_instances
                .lock()
                .unwrap_or_log()
                .push((instance, store));
        }

        Ok(results)
    }

    pub fn functions(&self) -> impl Iterator<Item = &FunctionFqn> {
        self.functions_to_metadata.keys()
    }
}

// TODO: implement Valuable for Cow<'_, str>
const _: () = {
    static FIELDS: &[::valuable::NamedField<'static>] = &[::valuable::NamedField::new("wasm_path")];
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
                &[::valuable::Value::String(&self.config.wasm_path)],
            ));
        }
    }
};

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{activity_engine, EngineConfig};
    use assert_matches::assert_matches;
    use concepts::{
        prefixed_ulid::ActivityId, ExecutionId, FunctionFqnStr, Params, SupportedFunctionResult,
    };
    use futures_util::StreamExt;
    use rstest::rstest;
    use scheduler::{
        executor::{ExecConfig, ExecTask},
        storage::{inmemory_dao::DbTask, DbConnection},
        FinishedExecutionError, FinishedExecutionResult,
    };
    use std::{
        borrow::Cow,
        sync::Arc,
        time::{Duration, Instant},
    };
    use test_utils::env_or_default;
    use tracing::{info, warn};
    use tracing_unwrap::{OptionExt, ResultExt};
    use utils::time::now;
    use wasmtime::component::Val;

    const FIBO_FFQN: FunctionFqnStr = FunctionFqnStr::new("testing:fibo/fibo", "fibo"); // func(n: u8) -> u64;

    #[tokio::test]
    async fn fibo_once() {
        const FIBO_INPUT: u8 = 40;
        const EXPECTED: u64 = 165580141;
        test_utils::set_up();
        let mut db_task = DbTask::spawn_new(1);
        let db_connection = db_task.as_db_connection().expect_or_log("must be open");

        let fibo_worker = ActivityWorker::new_with_config(
            db_connection.clone(),
            ActivityConfig {
                preload: true,
                wasm_path: Cow::Borrowed(
                    test_programs_fibo_activity_builder::TEST_PROGRAMS_FIBO_ACTIVITY,
                ),
                epoch_millis: 10,
            },
            activity_engine(EngineConfig::default()),
            None,
        )
        .unwrap_or_log();

        let exec_config = ExecConfig {
            ffqns: vec![FIBO_FFQN.to_owned()],
            batch_size: 1,
            lock_expiry: Duration::from_secs(1),
            lock_expiry_leeway: Duration::from_millis(100),
            tick_sleep: Duration::from_millis(100),
        };
        let exec_task = ExecTask::spawn_new(
            db_task.as_db_connection().unwrap_or_log(),
            fibo_worker,
            exec_config.clone(),
            Arc::new(tokio::sync::Semaphore::new(1)),
        );

        // Create an execution.
        let execution_id = ActivityId::generate();
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
            Ok(SupportedFunctionResult::Single(Val::U64(val))) => val);
        assert_eq!(EXPECTED, fibo);
        drop(db_connection);

        exec_task.close().await;
        db_task.close().await;
    }

    // FIXME: replace tracing-unwrap with this hook
    pub fn tracing_panic_hook(panic_info: &std::panic::PanicInfo) {
        let payload = panic_info.payload();

        #[allow(clippy::manual_map)]
        let payload = if let Some(s) = payload.downcast_ref::<&str>() {
            Some(&**s)
        } else if let Some(s) = payload.downcast_ref::<String>() {
            Some(s.as_str())
        } else {
            None
        };

        let location = panic_info.location().map(|l| l.to_string());

        let backtrace = std::backtrace::Backtrace::capture();
        if backtrace.status() == std::backtrace::BacktraceStatus::Captured {
            tracing::error!(
                panic.payload = payload,
                panic.location = location,
                "A panic occurred: {backtrace}"
            );
        } else {
            tracing::error!(
                panic.payload = payload,
                panic.location = location,
                "A panic occurred",
            );
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn fibo_parallel() {
        const FIBO_INPUT: u8 = 1;
        const EXECUTORS: u8 = 4;
        const EXECUTIONS: usize = 10;
        const RECYCLE: bool = false;
        const PERMITS: usize = 1000;
        const BATCH_SIZE: u32 = 1;
        const PRELOAD: bool = false;

        test_utils::set_up();
        std::panic::set_hook(Box::new(tracing_panic_hook));
        let fibo_input = env_or_default("FIBO_INPUT", FIBO_INPUT);
        let executors = env_or_default("EXECUTORS", EXECUTORS);
        let executions = env_or_default("EXECUTIONS", EXECUTIONS);
        let recycle_instances = env_or_default("RECYCLE", RECYCLE).then(Default::default);
        let permits = env_or_default("PERMITS", PERMITS);
        let batch_size = env_or_default("BATCH_SIZE", BATCH_SIZE);
        let preload = env_or_default("PRELOAD", PRELOAD);

        let mut db_task = DbTask::spawn_new(1);
        let db_connection = db_task.as_db_connection().expect_or_log("must be open");

        let fibo_worker = ActivityWorker::new_with_config(
            db_connection.clone(),
            ActivityConfig {
                preload,
                wasm_path: Cow::Borrowed(
                    test_programs_fibo_activity_builder::TEST_PROGRAMS_FIBO_ACTIVITY,
                ),
                epoch_millis: 10,
            },
            activity_engine(EngineConfig::default()),
            recycle_instances,
        )
        .unwrap_or_log();

        // spawn executors
        let task_limiter = Arc::new(tokio::sync::Semaphore::new(permits));
        let exec_tasks = (0..executors)
            .map(|_| {
                let exec_config = ExecConfig {
                    ffqns: vec![FIBO_FFQN.to_owned()],
                    batch_size,
                    lock_expiry: Duration::from_secs(1),
                    lock_expiry_leeway: Duration::from_millis(100),
                    tick_sleep: Duration::from_millis(10),
                };
                ExecTask::spawn_new(
                    db_task.as_db_connection().unwrap_or_log(),
                    fibo_worker.clone(),
                    exec_config.clone(),
                    task_limiter.clone(),
                )
            })
            .collect::<Vec<_>>();

        let stopwatch = Instant::now();
        // create executions
        let execution_ids = futures_util::stream::iter(0..executions)
            .map(|_| async {
                let execution_id = ActivityId::generate();
                let created_at = now();
                db_connection
                    .create(
                        created_at,
                        execution_id.clone(),
                        FIBO_FFQN.to_owned(),
                        Params::from([Val::U8(fibo_input)]),
                        None,
                        None,
                        Duration::ZERO,
                        0,
                    )
                    .await
                    .unwrap_or_log();
                execution_id
            })
            .buffer_unordered(executions)
            .collect::<Vec<_>>()
            .await;
        info!("Waiting for results");
        for execution_id in execution_ids {
            // Check that the computation succeded.
            assert_matches!(
                db_connection
                    .obtain_finished_result(execution_id)
                    .await
                    .unwrap_or_log(),
                Ok(SupportedFunctionResult::Single(Val::U64(_)))
            );
        }
        warn!("Finished in {} ms", stopwatch.elapsed().as_millis());
        drop(db_connection);
        for exec_task in exec_tasks {
            exec_task.close().await;
        }
        db_task.close().await;
    }

    const SLEEP_FFQN: FunctionFqnStr = FunctionFqnStr::new("testing:sleep/sleep", "sleep-loop"); // func(millis: u64, iterations: u32);
    #[rstest]
    #[case(100, Err(FinishedExecutionError::PermanentTimeout))] // 1s -> timeout
    #[case(10, Ok(SupportedFunctionResult::None))] // 0.1s -> Ok
    #[tokio::test]
    async fn sleep_timeout(
        #[case] sleep_iterations: u32,
        #[case] expected: FinishedExecutionResult<ActivityId>,
    ) {
        const SLEEP_MILLIS_PARAM: u64 = 10;
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
        let recycled_instances = Some(Default::default());
        let fibo_worker = ActivityWorker::new_with_config(
            db_connection.clone(),
            ActivityConfig {
                preload: true,
                wasm_path: Cow::Borrowed(
                    test_programs_sleep_activity_builder::TEST_PROGRAMS_SLEEP_ACTIVITY,
                ),
                epoch_millis: EPOCH_MILLIS,
            },
            engine,
            recycled_instances.clone(), // enable recycling
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
            Arc::new(tokio::sync::Semaphore::new(1)),
        );

        // Create an execution.
        let execution_id = ActivityId::generate();
        info!("Testing {execution_id}");
        let created_at = now();
        db_connection
            .create(
                created_at,
                execution_id.clone(),
                SLEEP_FFQN.to_owned(),
                Params::from([Val::U64(SLEEP_MILLIS_PARAM), Val::U32(sleep_iterations)]),
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
        assert_eq!(
            Some(if expected.is_ok() { 1 } else { 0 }),
            recycled_instances.as_ref().map(|i| i.lock().unwrap().len())
        );

        drop(db_connection);
        exec_task.close().await;
        db_task.close().await;
    }
}
