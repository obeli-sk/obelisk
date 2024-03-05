use crate::{
    storage::{DbConnection, HistoryEvent, Version},
    worker::{Worker, WorkerError},
};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use concepts::workflow_id::WorkflowId;
use concepts::{FunctionFqn, FunctionMetadata};
use concepts::{Params, SupportedFunctionResult};
use std::{borrow::Cow, collections::HashMap, error::Error, fmt::Debug, sync::Arc};
use tracing::{debug, enabled, trace, Level};
use tracing_unwrap::OptionExt;
use utils::wasm_tools;
use wasmtime::{component::Val, Engine};

#[derive(Debug, PartialEq, Eq)]
pub enum ActivityPreload {
    None,
    Preinstance,
    Instance,
}

impl Default for ActivityPreload {
    fn default() -> Self {
        Self::None
    }
}

#[derive(Debug, Default, PartialEq, Eq)]
#[allow(clippy::module_name_repetitions)]
pub struct ActivityConfig {
    pub preload: ActivityPreload,
}

enum ActivityPreloadHolder {
    None,
    Preinstance(wasmtime::component::InstancePre<utils::wasi_http::Ctx>),
    Instance(Option<wasmtime::component::Instance>),
}

struct ActivityWorker<DB: DbConnection<WorkflowId> + Send> {
    db_connection: DB,
    engine: Arc<Engine>,
    functions_to_metadata: HashMap<FunctionFqn, FunctionMetadata>,
    wasm_path: Arc<String>,
    preload_holder: ActivityPreloadHolder,
    linker: wasmtime::component::Linker<utils::wasi_http::Ctx>,
    component: wasmtime::component::Component,
}

#[derive(thiserror::Error, Debug)]
pub enum ActivityError {
    #[error("cannot open `{0}` - {1}")]
    CannotOpen(Arc<String>, std::io::Error),
    #[error("cannot decode `{0}` - {1}")]
    DecodeError(Arc<String>, wasm_tools::DecodeError),
    #[error("cannot decode metadata `{0}` - {1}")]
    FunctionMetadataError(Arc<String>, wasm_tools::FunctionMetadataError),
    #[error("cannot instantiate `{0}` - {1}")]
    InstantiationError(Arc<String>, Box<dyn Error>),
}

impl<DB: DbConnection<WorkflowId> + Send> ActivityWorker<DB> {
    #[tracing::instrument(skip_all, fields(wasm_path))]
    pub async fn new_with_config(
        db_connection: DB,
        wasm_path: Arc<String>,
        config: &ActivityConfig,
        engine: Arc<Engine>,
    ) -> Result<Self, ActivityError> {
        let wasm = std::fs::read(wasm_path.as_ref())
            .map_err(|err| ActivityError::CannotOpen(wasm_path.clone(), err))?;
        let (resolve, world_id) = wasm_tools::decode(&wasm)
            .map_err(|err| ActivityError::DecodeError(wasm_path.clone(), err))?;
        let exported_interfaces = wasm_tools::exported_ifc_fns(&resolve, &world_id)
            .map_err(|err| ActivityError::DecodeError(wasm_path.clone(), err))?;
        let functions_to_metadata = wasm_tools::functions_to_metadata(exported_interfaces)
            .map_err(|err| ActivityError::FunctionMetadataError(wasm_path.clone(), err))?;
        if enabled!(Level::TRACE) {
            trace!("Decoded functions {functions_to_metadata:#?}");
        } else {
            debug!("Decoded functions {:?}", functions_to_metadata.keys());
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
        let preload_holder = match config.preload {
            ActivityPreload::None => ActivityPreloadHolder::None,
            ActivityPreload::Preinstance => {
                let instance_pre: wasmtime::component::InstancePre<utils::wasi_http::Ctx> = {
                    linker.instantiate_pre(&component).map_err(|err| {
                        ActivityError::InstantiationError(wasm_path.clone(), err.into())
                    })?
                };
                ActivityPreloadHolder::Preinstance(instance_pre)
            }
            ActivityPreload::Instance => {
                let mut store = utils::wasi_http::store(&engine);
                let instance = linker
                    .instantiate_async(&mut store, &component)
                    .await
                    .map_err(|err| {
                        ActivityError::InstantiationError(wasm_path.clone(), err.into())
                    })?;
                ActivityPreloadHolder::Instance(Some(instance))
            }
        };
        Ok(Self {
            db_connection,
            engine,
            functions_to_metadata,
            preload_holder,
            wasm_path,
            linker,
            component,
        })
    }
}

#[async_trait]
impl<DB: DbConnection<WorkflowId> + Sync> Worker<WorkflowId> for ActivityWorker<DB> {
    async fn run(
        &mut self,
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

impl<DB: DbConnection<WorkflowId> + Sync> ActivityWorker<DB> {
    #[tracing::instrument(skip_all)]
    pub(crate) async fn run(
        &mut self,
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
        let instance;
        let instance = match &self.preload_holder {
            ActivityPreloadHolder::None => {
                instance = self
                    .linker
                    .instantiate_async(&mut store, &self.component)
                    .await
                    .map_err(|err| {
                        //TODO: limit reached etc
                        WorkerError::IntermittentError {
                            reason: Cow::Borrowed("cannot instantiate"),
                            err: err.into(),
                        }
                    })?;
                &instance
            }
            ActivityPreloadHolder::Preinstance(instance_pre) => {
                instance = instance_pre
                    .instantiate_async(&mut store)
                    .await
                    .map_err(|err| {
                        //TODO: limit reached etc
                        WorkerError::IntermittentError {
                            reason: Cow::Borrowed("cannot instantiate"),
                            err: err.into(),
                        }
                    })?;
                &instance
            }
            ActivityPreloadHolder::Instance(Some(instance)) => instance, // TODO: replace after guest panic
            ActivityPreloadHolder::Instance(None) => {
                instance = self
                    .linker
                    .instantiate_async(&mut store, &self.component)
                    .await
                    .map_err(|err| {
                        //TODO: limit reached etc
                        WorkerError::IntermittentError {
                            reason: Cow::Borrowed("cannot instantiate"),
                            err: err.into(),
                        }
                    })?;

                self.preload_holder = ActivityPreloadHolder::Instance(Some(instance));
                if let ActivityPreloadHolder::Instance(Some(instance)) = &self.preload_holder {
                    instance
                } else {
                    unreachable!("preload_holder was just created")
                }
            }
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
            })?;
        let results = SupportedFunctionResult::new(results);
        func.post_return_async(&mut store)
            .await
            .map_err(|err| WorkerError::IntermittentError {
                reason: Cow::Borrowed("wasm post function call error"),
                err: err.into(),
            })?;
        Ok(results)
    }

    pub fn functions(&self) -> impl Iterator<Item = &FunctionFqn> {
        self.functions_to_metadata.keys()
    }
}
