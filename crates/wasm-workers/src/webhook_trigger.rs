use concepts::storage::{ClientError, CreateRequest, DbConnection, DbError, DbPool};
use concepts::{
    ComponentConfigHash, ComponentType, ExecutionId, FinishedExecutionError, FunctionFqn,
    FunctionRegistry, Params, StrVariant,
};
use hyper::server::conn::http1;
use hyper_util::rt::TokioIo;
use std::marker::PhantomData;
use std::net::SocketAddr;
use std::ops::Deref;
use std::path::Path;
use std::time::Duration;
use std::{fmt::Debug, sync::Arc};
use tokio::net::TcpListener;
use tracing::{debug, error, info, instrument, trace};
use utils::time::ClockFn;
use utils::wasm_tools::WasmComponent;
use wasmtime::component::ResourceTable;
use wasmtime::component::{Linker, Val};
use wasmtime::PoolingAllocationConfig;
use wasmtime::{Engine, Store};
use wasmtime_wasi::{WasiCtx, WasiCtxBuilder, WasiView};
use wasmtime_wasi_http::bindings::http::types::Scheme;
use wasmtime_wasi_http::bindings::ProxyPre;
use wasmtime_wasi_http::body::HyperOutgoingBody;
use wasmtime_wasi_http::{WasiHttpCtx, WasiHttpView};

use crate::workflow_ctx::SUFFIX_PKG_EXT;
use crate::workflow_worker::HOST_ACTIVITY_IFC_STRING;
use crate::WasmFileError;

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct HttpTriggerConfig {
    pub config_id: ComponentConfigHash,
}
type StdError = Box<dyn std::error::Error + Send + Sync>;

#[derive(Debug, thiserror::Error)]
pub enum WebhookInstallationError {
    #[error("socket error: {0}")]
    SocketError(std::io::Error),
    #[error(transparent)]
    WasmFileError(#[from] WasmFileError),
    #[error("instantiation error: {0}")]
    InstantiationError(wasmtime::Error),
}

pub async fn server<C: ClockFn + 'static, DB: DbConnection + 'static, P: DbPool<DB> + 'static>(
    wasm_path: impl AsRef<Path>,
    addr: SocketAddr,
    db_pool: P,
    clock_fn: C,
    fn_registry: Arc<dyn FunctionRegistry>,
    retry_config: RetryConfig,
) -> Result<(), WebhookInstallationError> {
    let wasm_path = wasm_path.as_ref();

    let listener = TcpListener::bind(addr)
        .await
        .map_err(WebhookInstallationError::SocketError)?;
    let engine = {
        let mut wasmtime_config = wasmtime::Config::new();
        wasmtime_config.allocation_strategy(wasmtime::InstanceAllocationStrategy::Pooling(
            PoolingAllocationConfig::default(),
        ));
        wasmtime_config.wasm_component_model(true);
        // TODO epoch_interruption
        wasmtime_config.async_support(true);
        Arc::new(Engine::new(&wasmtime_config).unwrap())
    };
    let wasm_component =
        WasmComponent::new(wasm_path, &engine).map_err(|err| WasmFileError::DecodeError(err))?;
    let mut linker = Linker::new(&engine);
    wasmtime_wasi_http::add_to_linker_async(&mut linker).map_err(|err| {
        WasmFileError::LinkingError {
            context: StrVariant::Static("linking `wasmtime_wasi_http`"),
            err: err.into(),
        }
    })?;

    // Mock imported functions

    for import in &wasm_component.exim.imports_hierarchy {
        if import.ifc_fqn.deref() == HOST_ACTIVITY_IFC_STRING {
            // Skip host-implemented functions
            continue;
        }
        trace!(
            ifc_fqn = %import.ifc_fqn,
            "Adding imported interface to the linker",
        );
        if let Ok(mut linker_instance) = linker.instance(import.ifc_fqn.deref()) {
            for function_name in import.fns.keys() {
                let ffqn = FunctionFqn {
                    ifc_fqn: import.ifc_fqn.clone(),
                    function_name: function_name.clone(),
                };
                trace!("Adding mock for imported function {ffqn} to the linker");
                let res = linker_instance.func_new_async(function_name.deref(), {
                    let ffqn = ffqn.clone();
                    move |mut store_ctx: wasmtime::StoreContextMut<'_, WebhookCtx<C, DB, P>>,
                          params: &[Val],
                          results: &mut [Val]| {
                        let ffqn = ffqn.clone();
                        Box::new(async move {
                            Ok(store_ctx
                                .data_mut()
                                .call_imported_fn(ffqn, params, results)
                                .await?)
                        })
                    }
                });
                if let Err(err) = res {
                    if err.to_string() == format!("import `{function_name}` not found") {
                        debug!("Skipping mocking of {ffqn}");
                    } else {
                        return Err(WasmFileError::LinkingError {
                            context: StrVariant::Arc(Arc::from(format!(
                                "cannot add mock for imported function {ffqn}"
                            ))),
                            err: err.into(),
                        }
                        .into());
                    }
                }
            }
        } else {
            trace!("Skipping interface {ifc_fqn}", ifc_fqn = import.ifc_fqn);
        }
    }
    let instance = linker
        .instantiate_pre(&wasm_component.component)
        .map_err(WebhookInstallationError::InstantiationError)?;
    let instance = ProxyPre::new(instance).map_err(WebhookInstallationError::InstantiationError)?;
    loop {
        let (stream, _) = listener
            .accept()
            .await
            .map_err(WebhookInstallationError::SocketError)?;
        let io = TokioIo::new(stream);
        // Spawn a tokio task for each connection
        // TODO: cancel on connection drop
        tokio::task::spawn({
            let instance = instance.clone();
            let engine = engine.clone();
            let clock_fn = clock_fn.clone();
            let db_pool = db_pool.clone();
            let fn_registry = fn_registry.clone();
            async move {
                let res = http1::Builder::new()
                    .serve_connection(
                        io,
                        hyper::service::service_fn(move |req| {
                            handle_request(
                                req,
                                instance.clone(),
                                store(
                                    &engine,
                                    clock_fn.clone(),
                                    db_pool.clone(),
                                    fn_registry.clone(),
                                    retry_config,
                                ),
                            )
                        }),
                    )
                    .await;
                if let Err(err) = res {
                    error!("Error serving connection: {err:?}");
                }
            }
        });
    }
}

#[derive(Clone, Copy, Default)]
struct RetryConfig {
    activity_max_retries_override: Option<u32>,
    activity_retry_exp_backoff_override: Option<Duration>,
}

impl RetryConfig {
    fn max_retries(&self, component_type: ComponentType, component_default: u32) -> u32 {
        match component_type {
            ComponentType::WasmActivity => self
                .activity_max_retries_override
                .unwrap_or(component_default),
            ComponentType::WasmWorkflow => 0,
        }
    }

    fn retry_exp_backoff(
        &self,
        component_type: ComponentType,
        component_default: Duration,
    ) -> Duration {
        match component_type {
            ComponentType::WasmActivity => self
                .activity_retry_exp_backoff_override
                .unwrap_or(component_default),
            ComponentType::WasmWorkflow => Duration::ZERO,
        }
    }
}

struct WebhookCtx<C: ClockFn, DB: DbConnection, P: DbPool<DB>> {
    clock_fn: C,
    db_pool: P,
    fn_registry: Arc<dyn FunctionRegistry>,
    table: ResourceTable,
    wasi_ctx: WasiCtx,
    http_ctx: WasiHttpCtx,
    retry_config: RetryConfig,
    phantom_data: PhantomData<DB>,
}

#[derive(thiserror::Error, Debug, Clone)]
pub(crate) enum WebhookFunctionError {
    #[error("sumbitting failed, metadata for {ffqn} not found")]
    FunctionMetadataNotFound { ffqn: FunctionFqn },
    #[error(transparent)]
    DbError(#[from] DbError),
    #[error(transparent)]
    FinishedExecutionError(#[from] FinishedExecutionError),
    #[error("uncategorized error: {0}")]
    UncategorizedError(&'static str),
}

impl<C: ClockFn, DB: DbConnection, P: DbPool<DB>> WebhookCtx<C, DB, P> {
    #[instrument(skip_all, fields(%ffqn))]
    async fn call_imported_fn(
        &mut self,
        ffqn: FunctionFqn,
        params: &[Val],
        results: &mut [Val],
    ) -> Result<(), WebhookFunctionError> {
        debug!(?params, "call_imported_fn start");
        if let Some(_package_name) = ffqn.ifc_fqn.package_name().strip_suffix(SUFFIX_PKG_EXT) {
            unimplemented!("extensions are not implemented yet")
        }

        let execution_id = ExecutionId::generate();
        let created_at = (self.clock_fn)();
        let Some((function_metadata, config_id, default_retry_config)) =
            self.fn_registry.get_by_exported_function(&ffqn).await
        else {
            return Err(WebhookFunctionError::FunctionMetadataNotFound { ffqn });
        };
        let create_request = CreateRequest {
            created_at,
            execution_id,
            ffqn,
            params: Params::from_wasmtime(Arc::from(params)),
            parent: None,
            topmost_parent_id: None,
            scheduled_at: created_at,
            max_retries: self
                .retry_config
                .max_retries(config_id.component_type, default_retry_config.max_retries),
            retry_exp_backoff: self.retry_config.retry_exp_backoff(
                config_id.component_type,
                default_retry_config.retry_exp_backoff,
            ),
            config_id,
            return_type: function_metadata.return_type.map(|rt| rt.type_wrapper),
        };
        let conn = self.db_pool.connection();
        conn.create(create_request).await?;
        let res = match conn
            .wait_for_finished_result(execution_id, None /* TODO */)
            .await
        {
            Ok(res) => res.inspect_err(|err| error!("Got execution error: {err:?}"))?,
            Err(ClientError::DbError(err)) => return Err(WebhookFunctionError::DbError(err)),
            Err(ClientError::Timeout) => unreachable!("timeout was not set"),
        };
        if results.len() != res.len() {
            error!("Unexpected results length");
            return Err(WebhookFunctionError::UncategorizedError(
                "Unexpected results length",
            ));
        }
        for (idx, item) in res.value().into_iter().enumerate() {
            results[idx] = item.as_val();
        }
        trace!(?params, ?results, "call_imported_fn finish");
        Ok(())
    }
}

#[must_use]
fn store<C: ClockFn, DB: DbConnection, P: DbPool<DB>>(
    engine: &Engine,
    clock_fn: C,
    db_pool: P,
    fn_registry: Arc<dyn FunctionRegistry>,
    retry_config: RetryConfig,
) -> Store<WebhookCtx<C, DB, P>> {
    let mut builder = WasiCtxBuilder::new();
    let ctx = WebhookCtx {
        clock_fn,
        db_pool,
        fn_registry,
        table: ResourceTable::new(),
        wasi_ctx: builder.build(),
        http_ctx: WasiHttpCtx::new(),
        retry_config,
        phantom_data: PhantomData::default(),
    };
    Store::new(engine, ctx)
}

impl<C: ClockFn, DB: DbConnection, P: DbPool<DB>> WasiView for WebhookCtx<C, DB, P> {
    fn table(&mut self) -> &mut ResourceTable {
        &mut self.table
    }
    fn ctx(&mut self) -> &mut WasiCtx {
        &mut self.wasi_ctx
    }
}

impl<C: ClockFn, DB: DbConnection, P: DbPool<DB>> WasiHttpView for WebhookCtx<C, DB, P> {
    fn ctx(&mut self) -> &mut WasiHttpCtx {
        &mut self.http_ctx
    }

    fn table(&mut self) -> &mut ResourceTable {
        &mut self.table
    }
}

#[derive(Debug, thiserror::Error)]
pub enum HandleRequestError {
    #[error("incoming request error: {0}")]
    IncomingRequestError(StdError),
    #[error("response creation error: {0}")]
    ResponseCreationError(StdError),
    #[error("instantiation error: {0}")]
    InstantiationError(StdError),
    #[error("execution error: {0}")]
    ExecutionError(StdError),
}

async fn handle_request<
    C: ClockFn + 'static,
    DB: DbConnection + 'static,
    P: DbPool<DB> + 'static,
>(
    req: hyper::Request<hyper::body::Incoming>,
    instance: ProxyPre<WebhookCtx<C, DB, P>>,
    mut store: Store<WebhookCtx<C, DB, P>>,
) -> Result<hyper::Response<HyperOutgoingBody>, HandleRequestError> {
    let (sender, receiver) = tokio::sync::oneshot::channel();

    info!("Request {} to {}", req.method(), req.uri());

    // if self.run.common.wasm.timeout.is_some() {
    //     store.set_epoch_deadline(u64::from(EPOCH_PRECISION) + 1);
    // }

    let req = store
        .data_mut()
        .new_incoming_request(Scheme::Http, req)
        .map_err(|err| HandleRequestError::IncomingRequestError(err.into()))?;
    let out = store
        .data_mut()
        .new_response_outparam(sender)
        .map_err(|err| HandleRequestError::ResponseCreationError(err.into()))?;
    let proxy = instance
        .instantiate_async(&mut store)
        .await
        .map_err(|err| HandleRequestError::InstantiationError(err.into()))?;

    let task = tokio::task::spawn(async move {
        if let Err(e) = proxy
            .wasi_http_incoming_handler()
            .call_handle(store, req, out)
            .await
        {
            error!("{e:?}");
            return Err(e);
        }

        Ok(())
    });
    match receiver.await {
        Ok(Ok(resp)) => Ok(resp),
        Ok(Err(err)) => {
            error!("Webhook sent error {err:?}");
            Err(HandleRequestError::ExecutionError(err.into()))
        }
        Err(_recv_err) => {
            // An error in the receiver (`RecvError`) only indicates that the
            // task exited before a response was sent (i.e., the sender was
            // dropped); it does not describe the underlying cause of failure.
            // Instead we retrieve and propagate the error from inside the task
            // which should more clearly tell the user what went wrong. Note
            // that we assume the task has already exited at this point so the
            // `await` should resolve immediately.
            let err = match task.await {
                Ok(r) => r.expect_err("if the receiver has an error, the task must have failed"), //
                Err(e) => e.into(), // e.g. Panic
            };
            error!("Webhook instance error: {err:?}");
            Err(HandleRequestError::ExecutionError(err.into()))
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        activity_worker::tests::spawn_activity_fibo,
        tests::fn_registry_dummy,
        workflow_worker::{tests::spawn_workflow_fibo, JoinNextBlockingStrategy},
    };
    use concepts::FunctionFqn;
    use db_tests::Database;
    use std::net::SocketAddr;
    use test_utils::sim_clock::SimClock;

    struct AbortOnDrop<T>(tokio::task::JoinHandle<T>);
    impl<T> Drop for AbortOnDrop<T> {
        fn drop(&mut self) {
            self.0.abort();
        }
    }

    #[tokio::test]
    #[ignore = "server never exits"]
    async fn webhook_trigger_fibo() {
        test_utils::set_up();
        let addr = SocketAddr::from(([127, 0, 0, 1], 3000));
        let sim_clock = SimClock::default();
        let (_guard, db_pool) = Database::Memory.set_up().await;
        use test_programs_fibo_activity_builder::exports::testing::fibo::fibo::FIBO;
        use test_programs_fibo_workflow_builder::exports::testing::fibo_workflow::workflow::FIBOA;
        let activity_exec_task = spawn_activity_fibo(db_pool.clone(), sim_clock.get_clock_fn());
        let fn_registry = fn_registry_dummy(&[
            FunctionFqn::new_static(FIBOA.0, FIBOA.1),
            FunctionFqn::new_static(FIBO.0, FIBO.1),
        ]);
        let workflow_exec_task = spawn_workflow_fibo(
            db_pool.clone(),
            sim_clock.get_clock_fn(),
            JoinNextBlockingStrategy::Await,
            0,
            fn_registry.clone(),
        );
        //let server = AbortOnDrop(tokio::spawn(async move {
        super::server(
            test_programs_webhook_trigger_fibo_builder::TEST_PROGRAMS_WEBHOOK_TRIGGER_FIBO,
            addr,
            db_pool,
            sim_clock.get_clock_fn(),
            fn_registry,
            crate::webhook_trigger::RetryConfig::default(),
        )
        .await
        .unwrap();
        // }));
    }
}
