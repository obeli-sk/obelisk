use crate::EngineConfig;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use concepts::prefixed_ulid::{ConfigId, JoinSetId};
use concepts::{ExecutionId, FunctionFqn};
use concepts::{Params, SupportedFunctionResult};
use derivative::Derivative;
use scheduler::storage::DbConnection;
use scheduler::worker::{ChildExecutionRequest, FatalError};
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
use wasmtime::component::Linker;
use wasmtime::{component::Val, Engine};
use wasmtime::{Store, UpdateDeadline};

pub fn workflow_engine(config: EngineConfig) -> Arc<Engine> {
    let mut wasmtime_config = wasmtime::Config::new();
    wasmtime_config.wasm_backtrace_details(wasmtime::WasmBacktraceDetails::Disable);
    wasmtime_config.wasm_component_model(true);
    wasmtime_config.async_support(true);
    wasmtime_config.allocation_strategy(config.allocation_strategy);
    wasmtime_config.epoch_interruption(true);
    Arc::new(Engine::new(&wasmtime_config).unwrap_or_log())
}

#[derive(Derivative, Clone)]
#[derivative(Debug)]
pub struct WorkflowConfig {
    pub config_id: ConfigId,
    pub wasm_path: Cow<'static, str>,
    pub epoch_millis: u64,
    #[derivative(Debug = "ignore")]
    pub recycled_instances: MaybeRecycledInstances, // TODO: change type to an enum enabled/disabled
}

type MaybeRecycledInstances =
    Option<Arc<std::sync::Mutex<Vec<(wasmtime::component::Instance, Store<WorkflowCtx>)>>>>;

#[derive(Clone)]
struct WorkflowWorker<DB: DbConnection> {
    db_connection: DB,
    config: WorkflowConfig,
    engine: Arc<Engine>,
    exported_ffqns_to_results_len: HashMap<FunctionFqn, usize>,
    linker: wasmtime::component::Linker<WorkflowCtx>,
    component: wasmtime::component::Component,
}

#[derive(thiserror::Error, Debug)]
pub enum WorkflowError {
    #[error("cannot open `{0}` - {1}")]
    CannotOpen(Cow<'static, str>, std::io::Error),
    #[error("cannot decode `{0}` - {1}")]
    DecodeError(Cow<'static, str>, wasm_tools::DecodeError),
    #[error("cannot decode metadata `{0}` - {1}")]
    FunctionMetadataError(Cow<'static, str>, wasm_tools::FunctionMetadataError),
    #[error("cannot instantiate `{0}` - cannot add function `{1}` to linker - {2}")]
    PlumbingImportedFunctionError(Cow<'static, str>, FunctionFqn, Box<dyn Error>),
    #[error("cannot instantiate `{0}` - {1}")]
    InstantiationError(Cow<'static, str>, Box<dyn Error>),
}

// Generate `host_activities::Host` trait
wasmtime::component::bindgen!({
    path: "../../wit/workflow-engine/",
    async: true,
    interfaces: "import my-org:workflow-engine/host-activities;",
});

struct WorkflowCtx {
    execution_id: ExecutionId,
}
impl WorkflowCtx {
    fn replay_or_interrupt(
        &self,
        request: ChildExecutionRequest,
    ) -> Result<SupportedFunctionResult, HostFunctionError> {
        Err(HostFunctionError::Interrupt(request))
    }
}
#[async_trait::async_trait]
impl my_org::workflow_engine::host_activities::Host for WorkflowCtx {
    async fn noop(&mut self) -> wasmtime::Result<()> {
        Ok(())
    }
    async fn sleep(&mut self, millis: u64) -> wasmtime::Result<()> {
        Ok(())
    }
}
impl WorkflowCtx {
    pub(crate) fn add_to_linker(linker: &mut Linker<Self>) -> Result<(), wasmtime::Error> {
        my_org::workflow_engine::host_activities::add_to_linker(linker, |state: &mut Self| state)
    }
}

#[derive(thiserror::Error, Debug)]
enum HostFunctionError {
    #[error("non deterministic execution: `{0}`")]
    NonDeterminismDetected(Cow<'static, str>),
    #[error("interrupt: {}", .0.child_execution_id)]
    Interrupt(ChildExecutionRequest),
}

impl<DB: DbConnection> WorkflowWorker<DB> {
    #[tracing::instrument(skip_all, fields(wasm_path = %config.wasm_path, config_id = %config.config_id))]
    pub fn new_with_config(
        config: WorkflowConfig,
        engine: Arc<Engine>,
        db_connection: DB,
    ) -> Result<Self, WorkflowError> {
        info!("Reading");
        let wasm = std::fs::read(config.wasm_path.as_ref())
            .map_err(|err| WorkflowError::CannotOpen(config.wasm_path.clone(), err))?;
        let (resolve, world_id) = wasm_tools::decode(&wasm)
            .map_err(|err| WorkflowError::DecodeError(config.wasm_path.clone(), err))?;
        let exported_ffqns_to_results_len = {
            let exported_interfaces = wasm_tools::exported_ifc_fns(&resolve, &world_id)
                .map_err(|err| WorkflowError::DecodeError(config.wasm_path.clone(), err))?;
            trace!("Exported functions: {exported_interfaces:?}");
            wasm_tools::functions_and_result_lengths(exported_interfaces).map_err(|err| {
                WorkflowError::FunctionMetadataError(config.wasm_path.clone(), err)
            })?
        };
        debug!(?exported_ffqns_to_results_len, "Exported functions");
        let imported_interfaces = {
            let imp_fns_to_metadata = wasm_tools::functions_to_metadata(
                wasm_tools::imported_ifc_fns(&resolve, &world_id)
                    .map_err(|err| WorkflowError::DecodeError(config.wasm_path.clone(), err))?,
            )
            .map_err(|err| WorkflowError::FunctionMetadataError(config.wasm_path.clone(), err))?;
            let imp_ifcs_to_fn_names =
                wasm_tools::group_by_ifc_to_fn_names(imp_fns_to_metadata.keys());
            // TODO: remove host functions: imp_ifcs_to_fn_names.remove(HOST_ACTIVITY_IFC_STRING.deref().deref());
            imp_ifcs_to_fn_names
        };
        trace!("Imported interfaces: {imported_interfaces:?}");

        let mut linker = wasmtime::component::Linker::new(&engine);

        // Compile the wasm component
        let component =
            wasmtime::component::Component::from_binary(&engine, &wasm).map_err(|err| {
                WorkflowError::InstantiationError(config.wasm_path.clone(), err.into())
            })?;
        // Mock imported functions
        for (ifc_fqn, functions) in &imported_interfaces {
            trace!("Adding imported interface {ifc_fqn} to the linker");
            if let Ok(mut linker_instance) = linker.instance(ifc_fqn) {
                for function_name in functions {
                    let ffqn = FunctionFqn {
                        ifc_fqn: ifc_fqn.clone(),
                        function_name: function_name.clone(),
                    };
                    trace!("Adding imported function {ffqn} to the linker");
                    let res = linker_instance.func_new_async(&component, function_name, {
                        let ffqn = ffqn.clone();
                        move |store_ctx: wasmtime::StoreContextMut<'_, WorkflowCtx>,
                              params: &[Val],
                              results: &mut [Val]| {
                            let execution_id = store_ctx.data().execution_id.clone();
                            let ffqn = ffqn.clone();
                            Box::new(async move {
                                let request = ChildExecutionRequest {
                                    new_join_set_id: JoinSetId::generate(),
                                    child_execution_id: ExecutionId::generate(),
                                    ffqn: ffqn,
                                    params: Params::Vals(Arc::new(Vec::from(params))),
                                };
                                trace!("Child request {request:?}");
                                let res = store_ctx.data().replay_or_interrupt(request);
                                trace!("Response: {res:?}");
                                let res = res?;
                                assert_eq!(results.len(), res.len(), "unexpected results length");
                                for (idx, item) in res.value().into_iter().enumerate() {
                                    // results[idx] = item.val(); // needed `ty`
                                }
                                todo!()
                                // Ok(())
                            })
                        }
                    });
                    if let Err(err) = res {
                        if err.to_string() == format!("import `{function_name}` not found") {
                            debug!("Skipping plumbing of {ffqn}");
                        } else {
                            return Err(WorkflowError::PlumbingImportedFunctionError(
                                config.wasm_path.clone(),
                                ffqn,
                                err.into(),
                            ));
                        }
                    }
                }
            } else {
                trace!("Skipping interface {ifc_fqn}");
            }
        }
        Ok(Self {
            db_connection,
            config,
            engine,
            exported_ffqns_to_results_len,
            linker,
            component,
        })
    }
}

#[async_trait]
impl<DB: DbConnection> Worker for WorkflowWorker<DB> {
    async fn run(
        &self,
        execution_id: ExecutionId,
        ffqn: FunctionFqn,
        params: Params,
        events: Vec<HistoryEvent>,
        version: Version,
        execution_deadline: DateTime<Utc>,
    ) -> Result<(SupportedFunctionResult, Version), (WorkerError, Version)> {
        assert!(events.is_empty());
        self.run(execution_id, ffqn, params, execution_deadline)
            .await
            .map(|supported_result| (supported_result, version))
            .map_err(|err| match err {
                WorkerError::IntermittentError { err, reason } => {
                    match err
                        .source()
                        .and_then(|source| source.downcast_ref::<HostFunctionError>())
                    {
                        Some(HostFunctionError::NonDeterminismDetected(reason)) => (
                            WorkerError::FatalError(FatalError::NonDeterminismDetected(
                                reason.clone(),
                            )),
                            version,
                        ),
                        Some(HostFunctionError::Interrupt(request)) => {
                            (WorkerError::Interrupt(request.clone()), version)
                        }
                        None => (WorkerError::IntermittentError { err, reason }, version),
                    }
                }
                err => (err, version),
            })
    }
}

impl<DB: DbConnection> WorkflowWorker<DB> {
    #[tracing::instrument(skip_all)]
    pub(crate) async fn run(
        &self,
        execution_id: ExecutionId,
        ffqn: FunctionFqn,
        params: Params,
        execution_deadline: DateTime<Utc>,
    ) -> Result<SupportedFunctionResult, WorkerError> {
        let results_len = *self
            .exported_ffqns_to_results_len
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
                let ctx = WorkflowCtx { execution_id };
                let mut store = Store::new(&self.engine, ctx);
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
                debug!(duration = ?stopwatch.elapsed(), ?deadline_duration, %execution_deadline, res = ?res.as_ref().map(|_|()).map_err(|_|()), "Finished");
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
    impl<DB: DbConnection> ::valuable::Structable for WorkflowWorker<DB> {
        fn definition(&self) -> ::valuable::StructDef<'_> {
            ::valuable::StructDef::new_static("WorkflowWorker", ::valuable::Fields::Named(FIELDS))
        }
    }
    impl<DB: DbConnection> ::valuable::Valuable for WorkflowWorker<DB> {
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
    use super::*;
    use crate::{activity_worker::activity_engine, EngineConfig};
    use concepts::{prefixed_ulid::ConfigId, ExecutionId, FunctionFqnStr, Params};
    use scheduler::{
        executor::{ExecConfig, ExecTask, ExecutorTaskHandle},
        storage::{inmemory_dao::DbTask, journal::PendingState, DbConnection},
    };
    use std::{borrow::Cow, time::Duration};
    use tracing_unwrap::{OptionExt, ResultExt};
    use utils::time::now;
    use wasmtime::component::Val;

    pub const FIBO_WORKFLOW_FFQN: FunctionFqnStr =
        FunctionFqnStr::new("testing:fibo-workflow/workflow", "fiboa"); // fiboa: func(n: u8, iterations: u32) -> u64;

    pub(crate) fn spawn_workflow_fibo<DB: DbConnection>(db_connection: DB) -> ExecutorTaskHandle {
        let fibo_worker = WorkflowWorker::new_with_config(
            WorkflowConfig {
                wasm_path: Cow::Borrowed(
                    test_programs_fibo_workflow_builder::TEST_PROGRAMS_FIBO_WORKFLOW,
                ),
                epoch_millis: 10,
                config_id: ConfigId::generate(),
                recycled_instances: None,
            },
            activity_engine(EngineConfig::default()),
            db_connection.clone(),
        )
        .unwrap_or_log();

        let exec_config = ExecConfig {
            ffqns: vec![FIBO_WORKFLOW_FFQN.to_owned()],
            batch_size: 1,
            lock_expiry: Duration::from_secs(1),
            lock_expiry_leeway: Duration::from_millis(10),
            tick_sleep: Duration::from_millis(100),
        };
        ExecTask::spawn_new(db_connection, fibo_worker, exec_config.clone(), None)
    }

    #[tokio::test]
    async fn fibo_workflow_should_schedule_fibo_activity() {
        const INPUT_N: u8 = 10;
        const INPUT_ITERATIONS: u32 = 10;

        test_utils::set_up();
        let mut db_task = DbTask::spawn_new(1);
        let db_connection = db_task.as_db_connection().expect_or_log("must be open");
        let workflow_exec_task = spawn_workflow_fibo(db_connection.clone());
        // Create an execution.
        let execution_id = ExecutionId::generate();
        let created_at = now();
        db_connection
            .create(
                created_at,
                execution_id.clone(),
                FIBO_WORKFLOW_FFQN.to_owned(),
                Params::from([Val::U8(INPUT_N), Val::U32(INPUT_ITERATIONS)]),
                None,
                None,
                Duration::ZERO,
                0,
            )
            .await
            .unwrap_or_log();
        // Should end as BlockedByJoinSet
        db_connection
            .wait_for_pending_state(execution_id, PendingState::BlockedByJoinSet)
            .await
            .unwrap();
        drop(db_connection);

        workflow_exec_task.close().await;
        db_task.close().await;
    }
}
