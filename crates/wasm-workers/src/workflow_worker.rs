use crate::{EngineConfig, WasmFileError};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use concepts::prefixed_ulid::{ConfigId, DelayId, JoinSetId};
use concepts::{ExecutionId, FunctionFqn, StrVariant};
use concepts::{Params, SupportedFunctionResult};
use db::storage::AsyncResponse;
use db::storage::{HistoryEvent, Version};
use executor::worker::{ChildExecutionRequest, FatalError, SleepRequest};
use executor::worker::{Worker, WorkerError};
use rand::rngs::StdRng;
use rand::{RngCore, SeedableRng};
use std::collections::HashMap;
use std::ops::Deref;
use std::time::Duration;
use std::{fmt::Debug, sync::Arc};
use tracing::{debug, info, trace};
use utils::time::{now, now_tokio_instant};
use utils::wasm_tools;
use val_json::wast_val::val;
use wasmtime::component::Linker;
use wasmtime::{component::Val, Engine};
use wasmtime::{Store, UpdateDeadline};

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

#[derive(Debug, Clone)]
pub struct WorkflowConfig<C: Fn() -> DateTime<Utc> + Send + Sync + Clone + 'static> {
    pub config_id: ConfigId,
    pub wasm_path: StrVariant,
    pub epoch_millis: u64,
    pub clock_fn: C,
}

#[derive(Clone)]
struct WorkflowWorker<C: Fn() -> DateTime<Utc> + Send + Sync + Clone + 'static> {
    config: WorkflowConfig<C>,
    engine: Arc<Engine>,
    exported_ffqns_to_results_len: HashMap<FunctionFqn, usize>,
    linker: wasmtime::component::Linker<WorkflowCtx<C>>,
    component: wasmtime::component::Component,
}

// Generate `host_activities::Host` trait
wasmtime::component::bindgen!({
    path: "../../wit/workflow-engine/",
    async: true,
    interfaces: "import my-org:workflow-engine/host-activities;",
});

struct WorkflowCtx<C: Fn() -> DateTime<Utc> + Send + Sync + Clone + 'static> {
    execution_id: ExecutionId,
    events: Vec<HistoryEvent>,
    events_idx: usize,
    rng: StdRng,
    clock_fn: C,
}
impl<C: Fn() -> DateTime<Utc> + Send + Sync + Clone + 'static> WorkflowCtx<C> {
    fn replay_or_interrupt(
        &mut self,
        request: ChildExecutionRequest,
    ) -> Result<SupportedFunctionResult, FunctionError> {
        trace!(
            "Querying history for {request:?}, index: {}, history: {:?}",
            self.events_idx,
            self.events
        );

        while let Some(found) = self.events.get(self.events_idx) {
            match found {
                HistoryEvent::JoinSet { .. } => {
                    debug!("Skipping JoinSet");
                    self.events_idx += 1;
                }
                HistoryEvent::ChildExecutionAsyncRequest { .. } => {
                    debug!("Skipping ChildExecutionAsyncRequest");
                    self.events_idx += 1;
                }
                HistoryEvent::JoinNextBlocking { .. } => {
                    debug!("Skipping JoinNextBlocking");
                    self.events_idx += 1;
                }
                HistoryEvent::AsyncResponse {
                    response:
                        AsyncResponse::ChildExecutionAsyncResponse {
                            child_execution_id,
                            result,
                        },
                    ..
                } if *child_execution_id == request.child_execution_id => {
                    debug!("Found response in history: {found:?}");
                    self.events_idx += 1;
                    // TODO: Map FinishedExecutionError somehow
                    return Ok(result.clone().unwrap());
                }
                _ => {
                    panic!("{found:?}")
                }
            }
        }
        Err(FunctionError::ChildExecutionRequest(request))
    }

    fn next_u128(&mut self) -> u128 {
        let mut bytes = [0; 16];
        self.rng.fill_bytes(&mut bytes);
        u128::from_be_bytes(bytes)
    }

    fn add_to_linker(linker: &mut Linker<Self>) -> Result<(), wasmtime::Error> {
        my_org::workflow_engine::host_activities::add_to_linker(linker, |state: &mut Self| state)
    }
}
#[async_trait::async_trait]
impl<C: Fn() -> DateTime<Utc> + Send + Sync + Clone + 'static>
    my_org::workflow_engine::host_activities::Host for WorkflowCtx<C>
{
    async fn sleep(&mut self, millis: u64) -> wasmtime::Result<()> {
        let new_join_set_id =
            JoinSetId::from_parts(self.execution_id.timestamp(), self.next_u128());
        let delay_id = DelayId::from_parts(self.execution_id.timestamp(), self.next_u128());
        trace!(
            "Querying history for {delay_id}, index: {}, history: {:?}",
            self.events_idx,
            self.events
        );

        while let Some(found) = self.events.get(self.events_idx) {
            match found {
                HistoryEvent::JoinSet { .. } => {
                    debug!("Skipping JoinSet");
                    self.events_idx += 1;
                }
                HistoryEvent::DelayedUntilAsyncRequest { .. } => {
                    debug!("Skipping ChildExecutionAsyncRequest");
                    self.events_idx += 1;
                }
                HistoryEvent::JoinNextBlocking { .. } => {
                    debug!("Skipping JoinNextBlocking");
                    self.events_idx += 1;
                }
                HistoryEvent::AsyncResponse {
                    response:
                        AsyncResponse::DelayFinishedAsyncResponse {
                            delay_id: found_delay_id,
                        },
                    ..
                } if delay_id == *found_delay_id => {
                    debug!("Found response in history: {found:?}");
                    self.events_idx += 1;
                    return Ok(());
                }
                _ => {
                    panic!("{found:?}")
                }
            }
        }
        let expires_at = (self.clock_fn)() + Duration::from_millis(millis);
        Err(FunctionError::SleepRequest(SleepRequest {
            new_join_set_id,
            delay_id,
            expires_at,
        }))?
        // TODO: optimize for zero millis - write to db and continue
    }
}

#[derive(thiserror::Error, Debug)]
enum FunctionError {
    #[error("non deterministic execution: `{0}`")]
    NonDeterminismDetected(StrVariant),
    #[error("child({})", .0.child_execution_id)]
    ChildExecutionRequest(ChildExecutionRequest),
    #[error("sleep({})", .0.delay_id)]
    SleepRequest(SleepRequest),
}

const HOST_ACTIVITY_IFC_STRING: &str = "my-org:workflow-engine/host-activities";

impl<C: Fn() -> DateTime<Utc> + Send + Sync + Clone + 'static> WorkflowWorker<C> {
    #[tracing::instrument(skip_all, fields(wasm_path = %config.wasm_path, config_id = %config.config_id))]
    pub fn new_with_config(
        config: WorkflowConfig<C>,
        engine: Arc<Engine>,
    ) -> Result<Self, WasmFileError> {
        info!("Reading");
        let wasm = std::fs::read(config.wasm_path.deref())
            .map_err(|err| WasmFileError::CannotOpen(config.wasm_path.clone(), err))?;
        let (resolve, world_id) = wasm_tools::decode(&wasm)
            .map_err(|err| WasmFileError::DecodeError(config.wasm_path.clone(), err))?;
        let exported_ffqns_to_results_len = {
            let exported_interfaces = wasm_tools::exported_ifc_fns(&resolve, &world_id)
                .map_err(|err| WasmFileError::DecodeError(config.wasm_path.clone(), err))?;
            trace!("Exported functions: {exported_interfaces:?}");
            wasm_tools::functions_and_result_lengths(exported_interfaces).map_err(|err| {
                WasmFileError::FunctionMetadataError(config.wasm_path.clone(), err)
            })?
        };
        debug!(?exported_ffqns_to_results_len, "Exported functions");
        let imported_interfaces = {
            let imp_fns_to_metadata = wasm_tools::functions_to_metadata(
                wasm_tools::imported_ifc_fns(&resolve, &world_id)
                    .map_err(|err| WasmFileError::DecodeError(config.wasm_path.clone(), err))?,
            )
            .map_err(|err| WasmFileError::FunctionMetadataError(config.wasm_path.clone(), err))?;
            let mut imp_ifcs_to_fn_names =
                wasm_tools::group_by_ifc_to_fn_names(imp_fns_to_metadata.keys());
            // Remove host-implemented functions
            imp_ifcs_to_fn_names.remove(HOST_ACTIVITY_IFC_STRING);
            imp_ifcs_to_fn_names
        };
        trace!("Imported interfaces: {imported_interfaces:?}");

        let mut linker = wasmtime::component::Linker::new(&engine);

        // Compile the wasm component
        let component = wasmtime::component::Component::from_binary(&engine, &wasm)
            .map_err(|err| WasmFileError::CompilationError(config.wasm_path.clone(), err.into()))?;
        // Mock imported functions
        for (ifc_fqn, functions) in &imported_interfaces {
            trace!("Adding imported interface {ifc_fqn} to the linker");
            if let Ok(mut linker_instance) = linker.instance(ifc_fqn) {
                for function_name in functions {
                    let ffqn = FunctionFqn {
                        ifc_fqn: ifc_fqn.clone(),
                        function_name: function_name.clone(),
                    };
                    trace!("Adding mock for imported function {ffqn} to the linker");
                    let res = linker_instance.func_new_async(&component, function_name, {
                        let ffqn = ffqn.clone();
                        move |mut store_ctx: wasmtime::StoreContextMut<'_, WorkflowCtx<C>>,
                              params: &[Val],
                              results: &mut [Val]| {
                            let ffqn = ffqn.clone();
                            Box::new(async move {
                                let ctx = store_ctx.data_mut();
                                let request = ChildExecutionRequest {
                                    new_join_set_id: JoinSetId::from_parts(
                                        ctx.execution_id.timestamp(),
                                        ctx.next_u128(),
                                    ),
                                    child_execution_id: ExecutionId::from_parts(
                                        ctx.execution_id.timestamp(),
                                        ctx.next_u128(),
                                    ),
                                    ffqn,
                                    params: Params::Vals(Arc::new(Vec::from(params))),
                                };
                                let res = store_ctx.data_mut().replay_or_interrupt(request);
                                let res = res?;
                                assert_eq!(results.len(), res.len(), "unexpected results length");
                                for (idx, item) in res.value().into_iter().enumerate() {
                                    results[idx] = val(item, &results[idx].ty()).unwrap();
                                }
                                Ok(())
                            })
                        }
                    });
                    if let Err(err) = res {
                        if err.to_string() == format!("import `{function_name}` not found") {
                            debug!("Skipping mocking of {ffqn}");
                        } else {
                            return Err(WasmFileError::LinkingError {
                                file: config.wasm_path.clone(),
                                reason: StrVariant::Arc(Arc::from(format!(
                                    "cannot add mock for imported function {ffqn}"
                                ))),
                                err: err.into(),
                            });
                        }
                    }
                }
            } else {
                trace!("Skipping interface {ifc_fqn}");
            }
        }
        WorkflowCtx::add_to_linker(&mut linker).map_err(|err| WasmFileError::LinkingError {
            file: config.wasm_path.clone(),
            reason: StrVariant::Arc(Arc::from(format!("cannot add host activities"))),
            err: err.into(),
        })?;
        Ok(Self {
            config,
            engine,
            exported_ffqns_to_results_len,
            linker,
            component,
        })
    }
}

#[async_trait]
impl<C: Fn() -> DateTime<Utc> + Send + Sync + Clone + 'static> Worker for WorkflowWorker<C> {
    async fn run(
        &self,
        execution_id: ExecutionId,
        ffqn: FunctionFqn,
        params: Params,
        events: Vec<HistoryEvent>,
        version: Version,
        execution_deadline: DateTime<Utc>,
    ) -> Result<(SupportedFunctionResult, Version), (WorkerError, Version)> {
        self.run(execution_id, ffqn, params, events, execution_deadline)
            .await
            .map(|supported_result| (supported_result, version))
            .map_err(|err| match err {
                WorkerError::IntermittentError { err, reason } => {
                    match err
                        .source()
                        .and_then(|source| source.downcast_ref::<FunctionError>())
                    {
                        Some(FunctionError::NonDeterminismDetected(reason)) => (
                            WorkerError::FatalError(FatalError::NonDeterminismDetected(
                                reason.clone(),
                            )),
                            version,
                        ),
                        Some(FunctionError::ChildExecutionRequest(request)) => {
                            (WorkerError::ChildExecutionRequest(request.clone()), version)
                        }
                        Some(FunctionError::SleepRequest(request)) => {
                            (WorkerError::SleepRequest(request.clone()), version)
                        }
                        None => (WorkerError::IntermittentError { err, reason }, version),
                    }
                }
                err => (err, version),
            })
    }
}

impl<C: Fn() -> DateTime<Utc> + Send + Sync + Clone + 'static> WorkflowWorker<C> {
    #[tracing::instrument(skip_all)]
    pub(crate) async fn run(
        &self,
        execution_id: ExecutionId,
        ffqn: FunctionFqn,
        params: Params,
        events: Vec<HistoryEvent>,
        execution_deadline: DateTime<Utc>,
    ) -> Result<SupportedFunctionResult, WorkerError> {
        let results_len = *self
            .exported_ffqns_to_results_len
            .get(&ffqn)
            .expect("executor must only run existing functions");
        trace!("Params: {params:?}, results_len:{results_len}",);
        let (instance, mut store) = {
            let seed = execution_id.random() as u64;
            let ctx = WorkflowCtx {
                execution_id,
                events,
                events_idx: 0,
                rng: StdRng::seed_from_u64(seed),
                clock_fn: self.config.clock_fn.clone(),
            };
            let mut store = Store::new(&self.engine, ctx);
            let instance = self
                .linker
                .instantiate_async(&mut store, &self.component)
                .await
                .map_err(|err| WorkerError::IntermittentError {
                    reason: StrVariant::Static("cannot instantiate"),
                    err: err.into(),
                })?;
            (instance, store)
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
                if matches!(err.downcast_ref(), Some(wasmtime::Trap::OutOfFuel)) {
                    WorkerError::IntermittentTimeout { epoch_based: true }
                } else {
                    let err = err.into();
                    WorkerError::IntermittentError {
                        reason: StrVariant::Arc(Arc::from(format!(
                            "wasm function call error: `{err}`"
                        ))),
                        err,
                    }
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
            Ok(result)
        };
        let deadline_duration = (execution_deadline - now()).to_std().unwrap_or_default();
        let stopwatch = now_tokio_instant();
        let sleep_until = stopwatch + deadline_duration;
        tokio::select! {
            res = call_function =>{
                debug!(duration = ?stopwatch.elapsed(), ?deadline_duration, %execution_deadline, "Finished");
                res
            },
            _   = tokio::time::sleep_until(sleep_until) => {
                debug!(duration = ?stopwatch.elapsed(), ?deadline_duration, %execution_deadline, now = %now(), "Timed out");
                Err(WorkerError::IntermittentTimeout { epoch_based: false })
            }
        }
    }
}

mod valuable {
    use super::*;
    static FIELDS: &[::valuable::NamedField<'static>] = &[::valuable::NamedField::new("config_id")];
    impl<C: Fn() -> DateTime<Utc> + Send + Sync + Clone + 'static> ::valuable::Structable
        for WorkflowWorker<C>
    {
        fn definition(&self) -> ::valuable::StructDef<'_> {
            ::valuable::StructDef::new_static("WorkflowWorker", ::valuable::Fields::Named(FIELDS))
        }
    }
    impl<C: Fn() -> DateTime<Utc> + Send + Sync + Clone + 'static> ::valuable::Valuable
        for WorkflowWorker<C>
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

#[cfg(all(test))]
mod tests {
    // TODO: test epoch and select-based timeouts
    use super::*;
    use crate::{
        activity_worker::tests::{
            spawn_activity_fibo, EPOCH_MILLIS, FIBO_10_INPUT, FIBO_10_OUTPUT,
        },
        EngineConfig,
    };
    use assert_matches::assert_matches;
    use concepts::{prefixed_ulid::ConfigId, ExecutionId, Params};
    use db::storage::{inmemory_dao::DbTask, journal::PendingState, DbConnection};
    use executor::{
        executor::{ExecConfig, ExecTask, ExecutorTaskHandle},
        expired_timers_watcher,
    };
    use std::time::Duration;
    use test_utils::sim_clock::SimClock;
    use utils::time::now;
    use val_json::wast_val::WastVal;
    use wasmtime::component::Val;

    pub const FIBO_WORKFLOW_FFQN: FunctionFqn =
        FunctionFqn::new_static("testing:fibo-workflow/workflow", "fiboa"); // fiboa: func(n: u8, iterations: u32) -> u64;
    const SLEEP_HOST_ACTIVITY_FFQN: FunctionFqn = // TODO: generate in builder
        FunctionFqn::new_static("testing:sleep-workflow/workflow", "sleep-host-activity"); // sleep-host-activity: func(millis: u64);

    const TICK_SLEEP: Duration = Duration::from_millis(1);

    pub(crate) fn spawn_workflow_fibo<DB: DbConnection>(db_connection: DB) -> ExecutorTaskHandle {
        let fibo_worker = WorkflowWorker::new_with_config(
            WorkflowConfig {
                wasm_path: StrVariant::Static(
                    test_programs_fibo_workflow_builder::TEST_PROGRAMS_FIBO_WORKFLOW,
                ),
                epoch_millis: EPOCH_MILLIS,
                config_id: ConfigId::generate(),
                clock_fn: || now(),
            },
            workflow_engine(EngineConfig::default()),
        )
        .unwrap();

        let exec_config = ExecConfig {
            ffqns: vec![FIBO_WORKFLOW_FFQN.to_owned()],
            batch_size: 1,
            lock_expiry: Duration::from_secs(1),
            tick_sleep: TICK_SLEEP,
            clock_fn: || now(),
        };
        ExecTask::spawn_new(db_connection, fibo_worker, exec_config, None)
    }

    #[tokio::test]
    async fn fibo_workflow_should_schedule_fibo_activity() {
        const INPUT_ITERATIONS: u32 = 1;

        let _guard = test_utils::set_up();
        let mut db_task = DbTask::spawn_new(10);
        let db_connection = db_task.as_db_connection().expect("must be open");
        let workflow_exec_task = spawn_workflow_fibo(db_connection.clone());
        // Create an execution.
        let execution_id = ExecutionId::from_parts(0, 0);
        let created_at = now();
        db_connection
            .create(
                created_at,
                execution_id.clone(),
                FIBO_WORKFLOW_FFQN.to_owned(),
                Params::from([Val::U8(FIBO_10_INPUT), Val::U32(INPUT_ITERATIONS)]),
                None,
                None,
                Duration::ZERO,
                0,
            )
            .await
            .unwrap();
        // Should end as BlockedByJoinSet
        db_connection
            .wait_for_pending_state(execution_id.clone(), PendingState::BlockedByJoinSet)
            .await
            .unwrap();

        // Execution should call the activity and finish
        let activity_exec_task = spawn_activity_fibo(db_connection.clone());

        let res = db_connection
            .wait_for_finished_result(execution_id)
            .await
            .unwrap()
            .unwrap();
        let res = assert_matches!(res, SupportedFunctionResult::Infallible(val) => val);
        assert_eq!(
            FIBO_10_OUTPUT,
            assert_matches!(res, WastVal::U64(actual) => actual),
        );

        drop(db_connection);
        workflow_exec_task.close().await;
        activity_exec_task.close().await;
        db_task.close().await;
    }

    pub(crate) fn spawn_workflow_sleep<DB: DbConnection>(
        db_connection: DB,
        clock_fn: impl Fn() -> DateTime<Utc> + Send + Sync + Clone + 'static,
    ) -> ExecutorTaskHandle {
        let worker = WorkflowWorker::new_with_config(
            WorkflowConfig {
                wasm_path: StrVariant::Static(
                    test_programs_sleep_workflow_builder::TEST_PROGRAMS_SLEEP_WORKFLOW,
                ),
                epoch_millis: EPOCH_MILLIS,
                config_id: ConfigId::generate(),
                clock_fn: clock_fn.clone(),
            },
            workflow_engine(EngineConfig::default()),
        )
        .unwrap();

        let exec_config = ExecConfig {
            ffqns: vec![SLEEP_HOST_ACTIVITY_FFQN.to_owned()], // TODO get the list of functions from the worker
            batch_size: 1,
            lock_expiry: Duration::from_secs(1),
            tick_sleep: TICK_SLEEP,
            clock_fn,
        };
        ExecTask::spawn_new(db_connection, worker, exec_config, None)
    }

    #[tokio::test]
    async fn sleep_should_be_persisted_after_executor_restart() {
        const SLEEP_MILLIS: u64 = 100;
        let _guard = test_utils::set_up();
        let sim_clock = SimClock::new(now());
        let mut db_task = DbTask::spawn_new(10);
        let db_connection = db_task.as_db_connection().expect("must be open");
        let workflow_exec_task = spawn_workflow_sleep(db_connection.clone(), sim_clock.clock_fn());
        let timers_watcher_task = expired_timers_watcher::Task::spawn_new(
            db_connection.clone(),
            expired_timers_watcher::Config {
                tick_sleep: TICK_SLEEP,
                clock_fn: sim_clock.clock_fn(),
            },
        );
        let execution_id = ExecutionId::generate();
        db_connection
            .create(
                sim_clock.now(),
                execution_id.clone(),
                SLEEP_HOST_ACTIVITY_FFQN.to_owned(),
                Params::from([Val::U64(SLEEP_MILLIS)]),
                None,
                None,
                Duration::ZERO,
                0,
            )
            .await
            .unwrap();

        db_connection
            .wait_for_pending_state(execution_id.clone(), PendingState::BlockedByJoinSet)
            .await
            .unwrap();

        workflow_exec_task.close().await;
        sim_clock.sleep(Duration::from_millis(SLEEP_MILLIS));
        // Restart worker
        let workflow_exec_task = spawn_workflow_sleep(db_connection.clone(), sim_clock.clock_fn());
        let res = db_connection
            .wait_for_finished_result(execution_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(SupportedFunctionResult::None, res);
        drop(db_connection);
        workflow_exec_task.close().await;
        timers_watcher_task.close().await;
        db_task.close().await;
    }
}
