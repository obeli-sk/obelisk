use concepts::storage::{DbConnection, DbPool};
use concepts::{ComponentConfigHash, FunctionFqn, FunctionRegistry, StrVariant};
use hyper::server::conn::http1;
use hyper_util::rt::TokioIo;
use std::marker::PhantomData;
use std::net::SocketAddr;
use std::ops::Deref;
use std::path::Path;
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

use crate::workflow_ctx::FunctionError;
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

struct WebhookCtx<C: ClockFn, DB: DbConnection, P: DbPool<DB>> {
    clock_fn: C,
    db_pool: P,
    fn_registry: Arc<dyn FunctionRegistry>,
    table: ResourceTable,
    wasi_ctx: WasiCtx,
    http_ctx: WasiHttpCtx,
    phantom_data: PhantomData<DB>,
}

impl<C: ClockFn, DB: DbConnection, P: DbPool<DB>> WebhookCtx<C, DB, P> {
    #[instrument(skip_all, fields(%ffqn))]
    async fn call_imported_fn(
        &mut self,
        ffqn: FunctionFqn,
        params: &[Val],
        results: &mut [Val],
    ) -> Result<(), FunctionError> {
        debug!(?params, "call_imported_fn start");
        // let event_call = self.imported_fn_to_event_call(ffqn, params)?;
        // let res = self
        //     .event_history
        //     .replay_or_interrupt(
        //         event_call,
        //         &self.db_pool.connection(),
        //         &mut self.version,
        //         self.fn_registry.as_ref(),
        //     )
        //     .await?;
        // if results.len() != res.len() {
        //     error!("Unexpected results length");
        //     return Err(FunctionError::UncategorizedError(
        //         "Unexpected results length",
        //     ));
        // }
        // for (idx, item) in res.value().into_iter().enumerate() {
        //     results[idx] = item.as_val();
        // }
        results[0] = Val::U64(2); //TODO start a child execution, wait for its result
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
) -> Store<WebhookCtx<C, DB, P>> {
    let mut builder = WasiCtxBuilder::new();
    let ctx = WebhookCtx {
        clock_fn,
        db_pool,
        fn_registry,
        table: ResourceTable::new(),
        wasi_ctx: builder.build(),
        http_ctx: WasiHttpCtx::new(),
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
    use crate::tests::fn_registry_dummy;
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
        use test_programs_fibo_workflow_builder::exports::testing::fibo_workflow::workflow::FIBOA;
        let (_guard, db_pool) = Database::Memory.set_up().await;
        //let server = AbortOnDrop(tokio::spawn(async move {
        super::server(
            test_programs_webhook_trigger_fibo_builder::TEST_PROGRAMS_WEBHOOK_TRIGGER_FIBO,
            addr,
            db_pool,
            sim_clock.get_clock_fn(),
            fn_registry_dummy(&[FunctionFqn::new_static(FIBOA.0, FIBOA.1)]),
        )
        .await
        .unwrap();
        // }));
    }
}
