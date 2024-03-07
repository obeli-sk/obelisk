use async_trait::async_trait;
use chrono::{DateTime, Utc};
use concepts::workflow_id::WorkflowId;
use concepts::{FunctionFqn, FunctionMetadata};
use concepts::{Params, SupportedFunctionResult};
use scheduler::executor::WorkerId;
use scheduler::{
    storage::{DbConnection, HistoryEvent, Version},
    worker::{Worker, WorkerError},
};
use std::{borrow::Cow, collections::HashMap, error::Error, fmt::Debug, sync::Arc};
use tracing::{debug, enabled, info, info_span, trace, Level};
use tracing_unwrap::{OptionExt, ResultExt};
use utils::wasm_tools;
use wasmtime::{component::Val, Engine};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ActivityPreloadStrategy {
    None,
    Preinstance,
}

impl Default for ActivityPreloadStrategy {
    fn default() -> Self {
        Self::None
    }
}

#[derive(Debug, Default, PartialEq, Eq, Clone, Copy)]
#[allow(clippy::module_name_repetitions)]
pub struct ActivityConfig {
    pub preload: ActivityPreloadStrategy,
}

#[derive(Clone, derive_more::Display)]
#[display(fmt = "{id}")]
struct ActivityWorker {
    id: WorkerId,
    engine: Arc<Engine>,
    functions_to_metadata: HashMap<FunctionFqn, FunctionMetadata>,
    linker: wasmtime::component::Linker<utils::wasi_http::Ctx>,
    component: wasmtime::component::Component,
    instance_pre: Option<wasmtime::component::InstancePre<utils::wasi_http::Ctx>>,
    instances: Arc<std::sync::Mutex<Vec<wasmtime::component::Instance>>>,
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
    pub fn new_with_config<DB: DbConnection<WorkflowId>>(
        _db_connection: DB,
        wasm_path: Cow<'static, str>,
        config: ActivityConfig,
        engine: Arc<Engine>,
        instances: Arc<std::sync::Mutex<Vec<wasmtime::component::Instance>>>,
    ) -> Result<Self, ActivityError> {
        let id = WorkerId::new("act");
        info_span!("ActivityWorker", worker = %id).in_scope(|| {
            info!(%wasm_path, "Reading");
            let wasm = std::fs::read(wasm_path.as_ref())
                .map_err(|err| ActivityError::CannotOpen(wasm_path.clone(), err))?;
            let (resolve, world_id) = wasm_tools::decode(&wasm)
                .map_err(|err| ActivityError::DecodeError(wasm_path.clone(), err))?;
            let exported_interfaces = wasm_tools::exported_ifc_fns(&resolve, &world_id)
                .map_err(|err| ActivityError::DecodeError(wasm_path.clone(), err))?;
            let functions_to_metadata = wasm_tools::functions_to_metadata(exported_interfaces)
                .map_err(|err| ActivityError::FunctionMetadataError(wasm_path.clone(), err))?;
            if enabled!(Level::TRACE) {
                trace!(ffqns = ?functions_to_metadata.keys(), "Decoded functions {functions_to_metadata:#?}");
            } else {
                debug!(ffqns = ?functions_to_metadata.keys(), "Decoded functions" );
            }
            let mut linker = wasmtime::component::Linker::new(&engine);

            wasmtime_wasi::preview2::command::add_to_linker(&mut linker)
                .map_err(|err| ActivityError::InstantiationError(wasm_path.clone(), err.into()))?;
            wasmtime_wasi_http::bindings::http::outgoing_handler::add_to_linker(&mut linker, |t| t)
                .map_err(|err| ActivityError::InstantiationError(wasm_path.clone(), err.into()))?;
            wasmtime_wasi_http::bindings::http::types::add_to_linker(&mut linker, |t| t)
                .map_err(|err| ActivityError::InstantiationError(wasm_path.clone(), err.into()))?;
            // Compile the wasm component
            let component = wasmtime::component::Component::from_binary(&engine, &wasm)
                .map_err(|err| ActivityError::InstantiationError(wasm_path.clone(), err.into()))?;
            let instance_pre = match config.preload {
                ActivityPreloadStrategy::None => None,
                ActivityPreloadStrategy::Preinstance => {
                    Some(linker.instantiate_pre(&component).map_err(|err| {
                        ActivityError::InstantiationError(wasm_path.clone(), err.into())
                    })?)
                }
            };
            Ok(Self {
                id,
                engine,
                functions_to_metadata,
                linker,
                component,
                instance_pre,
                instances,
            })
        })
    }
}

#[async_trait]
impl Worker<WorkflowId> for ActivityWorker {
    async fn run(
        &self,
        _execution_id: WorkflowId,
        ffqn: FunctionFqn,
        params: Params,
        events: Vec<HistoryEvent<WorkflowId>>,
        version: Version,
        _lock_expires_at: DateTime<Utc>, // TODO
    ) -> Result<(SupportedFunctionResult, Version), (WorkerError, Version)> {
        assert!(events.is_empty());
        self.run(ffqn, params)
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
    ) -> Result<SupportedFunctionResult, WorkerError> {
        let results_len = self
            .functions_to_metadata
            .get(&ffqn)
            .expect_or_log("ffqn must be found")
            .results_len;
        trace!("Params: {params:?}, results_len:{results_len}",);
        let mut store = utils::wasi_http::store(&self.engine);
        let instance = self.instances.lock().unwrap_or_log().pop();
        let instance = match (instance, &self.instance_pre) {
            (Some(instance), _) => instance,
            (None, Some(instance_pre)) => instance_pre
                .instantiate_async(&mut store)
                .await
                .map_err(|err| WorkerError::IntermittentError {
                    reason: Cow::Borrowed("cannot instantiate"),
                    err: err.into(),
                })?,
            (None, None) => self
                .linker
                .instantiate_async(&mut store, &self.component)
                .await
                .map_err(|err| WorkerError::IntermittentError {
                    reason: Cow::Borrowed("cannot instantiate"),
                    err: err.into(),
                })?,
        };
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
            .map_err(|err| WorkerError::IntermittentError {
                reason: Cow::Borrowed("wasm function call error"),
                err: err.into(),
            })?; // guest panic exits here
        let results = SupportedFunctionResult::new(results);
        func.post_return_async(&mut store)
            .await
            .map_err(|err| WorkerError::IntermittentError {
                reason: Cow::Borrowed("wasm post function call error"),
                err: err.into(),
            })?;

        self.instances.lock().unwrap_or_log().push(instance);

        Ok(results)
    }

    pub fn functions(&self) -> impl Iterator<Item = &FunctionFqn> {
        self.functions_to_metadata.keys()
    }
}

#[cfg(test)]
mod tests {
    use std::{borrow::Cow, sync::Arc, time::Duration};

    use assert_matches::assert_matches;
    use chrono::TimeDelta;
    use concepts::{workflow_id::WorkflowId, FunctionFqnStr, Params, SupportedFunctionResult};
    use scheduler::{
        executor::{ExecConfig, ExecTask},
        storage::{
            inmemory_dao::DbTask, journal::PendingState, DbConnection, ExecutionEvent,
            ExecutionEventInner,
        },
    };
    use tracing::debug;
    use tracing_unwrap::{OptionExt, ResultExt};
    use utils::time::now;
    use wasmtime::component::Val;

    use crate::{activity_engine, EngineConfig};

    use super::{ActivityConfig, ActivityWorker};

    const FIBO_FFQN: FunctionFqnStr = FunctionFqnStr::new("testing:fibo/fibo", "fibo"); // func(n: u8) -> u64;

    #[tokio::test]
    async fn fibo() {
        test_utils::set_up();

        let mut db_task = DbTask::spawn_new(1);
        let db_connection = db_task.as_db_connection().expect_or_log("must be open");

        let fibo_worker = ActivityWorker::new_with_config(
            db_connection.clone(),
            Cow::Borrowed(test_programs_fibo_activity_builder::TEST_PROGRAMS_FIBO_ACTIVITY),
            ActivityConfig::default(),
            activity_engine(EngineConfig::default()),
            Default::default(),
        )
        .unwrap_or_log();

        let exec_config = ExecConfig {
            ffqns: vec![FIBO_FFQN.to_owned()],
            batch_size: 1,
            lock_expiry: Duration::from_secs(1),
            max_tick_sleep: Duration::from_millis(500),
            max_retries: 1,
            retry_exp_backoff: TimeDelta::zero(),
        };
        let exec_task = ExecTask::spawn_new(
            db_task.as_db_connection().unwrap_or_log(),
            fibo_worker,
            exec_config.clone(),
            Arc::new(tokio::sync::Semaphore::new(1)),
        );

        // Create an execution
        let execution_id = WorkflowId::generate();
        let created_at = now();
        db_connection
            .create(
                created_at,
                execution_id.clone(),
                FIBO_FFQN.to_owned(),
                Params::from([Val::U8(5)]),
                None,
                None,
            )
            .await
            .unwrap_or_log();

        let execution_events = loop {
            let (execution_events, _, pending_state) = db_connection
                .get(execution_id.clone())
                .await
                .unwrap_or_log();
            if pending_state == PendingState::Finished {
                break execution_events;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        };

        let fibo = *assert_matches!(execution_events.last().unwrap_or_log(),
            ExecutionEvent { event: ExecutionEventInner::Finished { result: Ok(SupportedFunctionResult::Single(Val::U64(result))) }, .. } => result);
        assert_eq!(8, fibo);
        drop(db_connection);

        exec_task.close().await;
        db_task.close().await;
    }
}
