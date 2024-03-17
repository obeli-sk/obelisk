use crate::EngineConfig;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use concepts::prefixed_ulid::{ConfigId, WorkflowId};
use concepts::FunctionFqn;
use concepts::{Params, SupportedFunctionResult};
use scheduler::storage::DbConnection;
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

pub fn workflow_engine(config: EngineConfig) -> Arc<Engine> {
    let mut wasmtime_config = wasmtime::Config::new();
    wasmtime_config.wasm_backtrace_details(wasmtime::WasmBacktraceDetails::Disable);
    wasmtime_config.wasm_component_model(true);
    wasmtime_config.async_support(true);
    wasmtime_config.allocation_strategy(config.allocation_strategy);
    wasmtime_config.epoch_interruption(true);
    Arc::new(Engine::new(&wasmtime_config).unwrap_or_log())
}

#[derive(Debug, PartialEq, Eq, Clone)]
#[allow(clippy::module_name_repetitions)]
pub struct WorkflowConfig {
    pub config_id: ConfigId,
    pub wasm_path: Cow<'static, str>,
    pub epoch_millis: u64,
}

type MaybeRecycledInstances = Option<
    Arc<std::sync::Mutex<Vec<(wasmtime::component::Instance, Store<utils::wasi_http::Ctx>)>>>,
>;

#[derive(Clone)]
struct WorkflowWorker<DB: DbConnection<WorkflowId>> {
    db_connection: DB,
    config: WorkflowConfig,
    engine: Arc<Engine>,
    ffqns_to_results_len: HashMap<FunctionFqn, usize>,
    linker: wasmtime::component::Linker<utils::wasi_http::Ctx>,
    component: wasmtime::component::Component,
    recycled_instances: MaybeRecycledInstances,
}

#[derive(thiserror::Error, Debug)]
pub enum WorkflowError {
    #[error("cannot open `{0}` - {1}")]
    CannotOpen(Cow<'static, str>, std::io::Error),
    #[error("cannot decode `{0}` - {1}")]
    DecodeError(Cow<'static, str>, wasm_tools::DecodeError),
    #[error("cannot decode metadata `{0}` - {1}")]
    FunctionMetadataError(Cow<'static, str>, wasm_tools::FunctionMetadataError),
    #[error("cannot instantiate `{0}` - {1}")]
    InstantiationError(Cow<'static, str>, Box<dyn Error>),
}

impl<DB: DbConnection<WorkflowId>> WorkflowWorker<DB> {
    #[tracing::instrument(skip_all, fields(wasm_path = %config.wasm_path, config_id = %config.config_id))]
    pub fn new_with_config(
        config: WorkflowConfig,
        engine: Arc<Engine>,
        recycled_instances: MaybeRecycledInstances,
        db_connection: DB,
    ) -> Result<Self, WorkflowError> {
        info!("Reading");
        let wasm = std::fs::read(config.wasm_path.as_ref())
            .map_err(|err| WorkflowError::CannotOpen(config.wasm_path.clone(), err))?;
        let (resolve, world_id) = wasm_tools::decode(&wasm)
            .map_err(|err| WorkflowError::DecodeError(config.wasm_path.clone(), err))?;
        let exported_interfaces = wasm_tools::exported_ifc_fns(&resolve, &world_id)
            .map_err(|err| WorkflowError::DecodeError(config.wasm_path.clone(), err))?;
        let ffqns_to_results_len = wasm_tools::functions_and_result_lengths(exported_interfaces)
            .map_err(|err| WorkflowError::FunctionMetadataError(config.wasm_path.clone(), err))?;

        debug!(?ffqns_to_results_len, "Decoded functions");
        let mut linker = wasmtime::component::Linker::new(&engine);

        wasmtime_wasi::preview2::command::add_to_linker(&mut linker).map_err(|err| {
            WorkflowError::InstantiationError(config.wasm_path.clone(), err.into())
        })?;
        wasmtime_wasi_http::bindings::http::outgoing_handler::add_to_linker(&mut linker, |t| t)
            .map_err(|err| {
                WorkflowError::InstantiationError(config.wasm_path.clone(), err.into())
            })?;
        wasmtime_wasi_http::bindings::http::types::add_to_linker(&mut linker, |t| t).map_err(
            |err| WorkflowError::InstantiationError(config.wasm_path.clone(), err.into()),
        )?;
        // Compile the wasm component
        let component =
            wasmtime::component::Component::from_binary(&engine, &wasm).map_err(|err| {
                WorkflowError::InstantiationError(config.wasm_path.clone(), err.into())
            })?;
        Ok(Self {
            db_connection,
            config,
            engine,
            ffqns_to_results_len,
            linker,
            component,
            recycled_instances,
        })
    }
}

#[async_trait]
impl<DB: DbConnection<WorkflowId> + Sync> Worker<WorkflowId> for WorkflowWorker<DB> {
    async fn run(
        &self,
        _execution_id: WorkflowId,
        ffqn: FunctionFqn,
        params: Params,
        events: Vec<HistoryEvent<WorkflowId>>,
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

impl<DB: DbConnection<WorkflowId>> WorkflowWorker<DB> {
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

            if let Some(recycled_instances) = &self.recycled_instances {
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
    impl<DB: DbConnection<WorkflowId>> ::valuable::Structable for WorkflowWorker<DB> {
        fn definition(&self) -> ::valuable::StructDef<'_> {
            ::valuable::StructDef::new_static("WorkflowWorker", ::valuable::Fields::Named(FIELDS))
        }
    }
    impl<DB: DbConnection<WorkflowId>> ::valuable::Valuable for WorkflowWorker<DB> {
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
mod tests {
    use super::*;
    use crate::{
        activity_worker::{activity_engine, tests::FIBO_FFQN, ActivityConfig, ActivityWorker},
        EngineConfig,
    };
    use assert_matches::assert_matches;
    use concepts::{
        prefixed_ulid::{ActivityId, ConfigId},
        ExecutionId, FunctionFqnStr, Params, SupportedFunctionResult,
    };
    use rstest::rstest;
    use scheduler::{
        executor::{ExecConfig, ExecTask},
        storage::{inmemory_dao::DbTask, DbConnection},
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

    #[tokio::test]
    async fn fibo() {
        const FIBO_INPUT: u8 = 40;
        const EXPECTED: u64 = 165580141;
        test_utils::set_up();
        let mut db_task = DbTask::spawn_new(1);
        let db_connection = db_task.as_db_connection().expect_or_log("must be open");

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
        let activity_exec_config = ExecConfig {
            ffqns: vec![FIBO_FFQN.to_owned()],
            batch_size: 1,
            lock_expiry: Duration::from_secs(1),
            lock_expiry_leeway: Duration::from_millis(10),
            tick_sleep: Duration::from_millis(100),
        };
        let activity_exec_task = ExecTask::spawn_new(
            db_task.as_db_connection().unwrap_or_log(),
            fibo_worker,
            activity_exec_config.clone(),
            None,
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
            Ok(SupportedFunctionResult::Infallible(WastVal::U64(val))) => val);
        assert_eq!(EXPECTED, fibo);
        drop(db_connection);

        activity_exec_task.close().await;
        db_task.close().await;
    }
}
