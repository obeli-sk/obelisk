// TODO: Execution ID/Request ID, Component ID?, Config ID, tracing. Persisted with first DB write.
// Add Host Activities
// Add _submit + _await functions
// Test outbound HTTP, IO?
// Timeouts
// Panic - propagate reason
// stdout and stderr

use crate::workflow_ctx::SUFFIX_PKG_EXT;
use crate::workflow_worker::HOST_ACTIVITY_IFC_STRING;
use crate::WasmFileError;
use concepts::storage::{ClientError, CreateRequest, DbConnection, DbError, DbPool};
use concepts::{
    ComponentType, ConfigId, ExecutionId, FinishedExecutionError, FunctionFqn, FunctionRegistry,
    Params, StrVariant,
};
use http_body_util::combinators::BoxBody;
use hyper::body::Bytes;
use hyper::server::conn::http1;
use hyper::{Method, StatusCode, Uri};
use hyper_util::rt::TokioIo;
use route_recognizer::{Match, Router};
use std::marker::PhantomData;
use std::ops::Deref;
use std::time::Duration;
use std::{fmt::Debug, sync::Arc};
use tokio::net::TcpListener;
use tracing::{debug, error, info, instrument, trace};
use utils::time::ClockFn;
use utils::wasm_tools::WasmComponent;
use wasmtime::component::ResourceTable;
use wasmtime::component::{Linker, Val};
use wasmtime::{Engine, Store};
use wasmtime_wasi::{WasiCtx, WasiCtxBuilder, WasiView};
use wasmtime_wasi_http::bindings::http::types::Scheme;
use wasmtime_wasi_http::bindings::ProxyPre;
use wasmtime_wasi_http::body::HyperOutgoingBody;
use wasmtime_wasi_http::{WasiHttpCtx, WasiHttpView};

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct HttpTriggerConfig {
    pub config_id: ConfigId,
}
type StdError = Box<dyn std::error::Error + Send + Sync>;

#[derive(Debug, thiserror::Error)]
pub enum WebhookComponentInstantiationError {
    #[error(transparent)]
    WasmFileError(#[from] WasmFileError),
    #[error("instantiation error: {0}")]
    InstantiationError(wasmtime::Error),
}

#[derive(Debug, thiserror::Error)]
pub enum WebhookServerError {
    #[error("socket error: {0}")]
    SocketError(std::io::Error),
}

pub struct WebhookInstance<C: ClockFn, DB: DbConnection, P: DbPool<DB>>(
    Arc<ProxyPre<WebhookCtx<C, DB, P>>>,
);

impl<C: ClockFn, DB: DbConnection, P: DbPool<DB>> Clone for WebhookInstance<C, DB, P> {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

pub struct MethodAwareRouter<T> {
    method_map: hashbrown::HashMap<Method, Router<T>>,
    fallback: Router<T>,
}

impl<T: Clone> MethodAwareRouter<T> {
    pub fn add(&mut self, method: Option<Method>, route: &str, dest: T) {
        if route.is_empty() {
            // When the route is empty, interpret it as matching all paths:
            self.add(method.clone(), "/", dest.clone());
            self.add(method, "/*", dest);
        } else if let Some(method) = method {
            self.method_map.entry(method).or_default().add(route, dest);
        } else {
            self.fallback.add(route, dest);
        }
    }

    fn find(&self, method: &Method, path: &Uri) -> Option<Match<&T>> {
        let path = path.path();
        self.method_map
            .get(method)
            .and_then(|router| router.recognize(path).ok())
            .or_else(|| {
                let fallback = self.fallback.recognize(path).ok();
                fallback
            })
    }
}

impl<T> Default for MethodAwareRouter<T> {
    fn default() -> Self {
        Self {
            method_map: hashbrown::HashMap::default(),
            fallback: Router::default(),
        }
    }
}

pub fn component_to_instance<
    C: ClockFn + 'static,
    DB: DbConnection + 'static,
    P: DbPool<DB> + 'static,
>(
    wasm_component: &WasmComponent,
    engine: &Engine,
) -> Result<WebhookInstance<C, DB, P>, WebhookComponentInstantiationError> {
    let mut linker = Linker::new(engine);
    wasmtime_wasi::add_to_linker_async(&mut linker).map_err(|err| WasmFileError::LinkingError {
        context: StrVariant::Static("linking `wasmtime_wasi`"),
        err: err.into(),
    })?;
    wasmtime_wasi_http::add_only_http_to_linker_async(&mut linker).map_err(|err| {
        WasmFileError::LinkingError {
            context: StrVariant::Static("linking `wasmtime_wasi_http`"),
            err: err.into(),
        }
    })?;
    // wasmtime_wasi_http::add_to_linker_async(&mut linker).map_err(|err| {
    //     WasmFileError::LinkingError {
    //         context: StrVariant::Static("linking `wasmtime_wasi_http`"),
    //         err: err.into(),
    //     }
    // })?;
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
                        return Err(WebhookComponentInstantiationError::WasmFileError(
                            WasmFileError::LinkingError {
                                context: StrVariant::Arc(Arc::from(format!(
                                    "cannot add mock for imported function {ffqn}"
                                ))),
                                err: err.into(),
                            },
                        ));
                    }
                }
            }
        } else {
            trace!("Skipping interface {ifc_fqn}", ifc_fqn = import.ifc_fqn);
        }
    }
    let instance = linker
        .instantiate_pre(&wasm_component.component)
        .map_err(WebhookComponentInstantiationError::InstantiationError)?;
    let instance =
        ProxyPre::new(instance).map_err(WebhookComponentInstantiationError::InstantiationError)?;
    Ok(WebhookInstance(Arc::new(instance)))
}

pub async fn server<C: ClockFn + 'static, DB: DbConnection + 'static, P: DbPool<DB> + 'static>(
    listener: TcpListener,
    engine: Arc<Engine>,
    router: MethodAwareRouter<WebhookInstance<C, DB, P>>,
    db_pool: P,
    clock_fn: C,
    fn_registry: Arc<dyn FunctionRegistry>,
    retry_config: RetryConfig,
) -> Result<(), WebhookServerError> {
    let router = Arc::new(router);
    loop {
        let (stream, _) = listener
            .accept()
            .await
            .map_err(WebhookServerError::SocketError)?;
        let io = TokioIo::new(stream);
        // Spawn a tokio task for each connection
        // TODO: cancel on connection drop and on server exit
        tokio::task::spawn({
            let router = router.clone();
            let engine = engine.clone();
            let clock_fn = clock_fn.clone();
            let db_pool = db_pool.clone();
            let fn_registry = fn_registry.clone();
            async move {
                let res = http1::Builder::new()
                    .serve_connection(
                        io,
                        hyper::service::service_fn(move |req| {
                            debug!("method: {}, uri: {}", req.method(), req.uri());
                            handle_request(
                                req,
                                router.clone(),
                                engine.clone(),
                                clock_fn.clone(),
                                db_pool.clone(),
                                fn_registry.clone(),
                                retry_config,
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
pub struct RetryConfig {
    activity_max_retries_override: Option<u32>,
    activity_retry_exp_backoff_override: Option<Duration>,
}

impl RetryConfig {
    fn max_retries(&self, component_type: ComponentType, component_default: u32) -> u32 {
        match component_type {
            ComponentType::WasmActivity => self
                .activity_max_retries_override
                .unwrap_or(component_default),
            ComponentType::Workflow => 0,
            ComponentType::Webhook => unreachable!("webhook can only be invoked via HTTP"),
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
            ComponentType::Workflow => Duration::ZERO,
            ComponentType::Webhook => unreachable!("webhook can only be invoked via HTTP"),
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

impl<C: ClockFn, DB: DbConnection, P: DbPool<DB>> WebhookCtx<C, DB, P> {
    #[must_use]
    fn new<'a>(
        engine: &Engine,
        clock_fn: C,
        db_pool: P,
        fn_registry: Arc<dyn FunctionRegistry>,
        retry_config: RetryConfig,
        params: impl Iterator<Item = (&'a str, &'a str)>,
    ) -> Store<WebhookCtx<C, DB, P>> {
        let mut wasi_ctx = WasiCtxBuilder::new();
        for (key, val) in params {
            wasi_ctx.env(key, val);
        }
        let wasi_ctx = wasi_ctx.build();
        let ctx = WebhookCtx {
            clock_fn,
            db_pool,
            fn_registry,
            table: ResourceTable::new(),
            wasi_ctx,
            http_ctx: WasiHttpCtx::new(),
            retry_config,
            phantom_data: PhantomData,
        };
        Store::new(engine, ctx)
    }
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

async fn handle_request<
    C: ClockFn + 'static,
    DB: DbConnection + 'static,
    P: DbPool<DB> + 'static,
>(
    req: hyper::Request<hyper::body::Incoming>,
    router: Arc<MethodAwareRouter<WebhookInstance<C, DB, P>>>,
    engine: Arc<Engine>,
    clock_fn: C,
    db_pool: P,
    fn_registry: Arc<dyn FunctionRegistry>,
    retry_config: RetryConfig,
) -> Result<hyper::Response<HyperOutgoingBody>, hyper::Error> {
    handle_request_inner(
        req,
        router,
        engine,
        clock_fn,
        db_pool,
        fn_registry,
        retry_config,
    )
    .await
    .or_else(|err| {
        fn resp(body: &str, status_code: StatusCode) -> hyper::Response<HyperOutgoingBody> {
            let body = BoxBody::new(http_body_util::BodyExt::map_err(
                http_body_util::Full::new(Bytes::copy_from_slice(body.as_bytes())),
                |_| unreachable!(),
            ));
            hyper::Response::builder()
                .status(status_code)
                .body(body)
                .unwrap()
        }
        debug!("{err:?}");
        Ok(match err {
            HandleRequestError::IncomingRequestError(err) => resp(
                &format!("Incoming request error: {err}"),
                StatusCode::BAD_REQUEST,
            ),
            HandleRequestError::ResponseCreationError(err) => resp(
                &format!("Cannot create response: {err}"),
                StatusCode::INTERNAL_SERVER_ERROR,
            ),
            HandleRequestError::InstantiationError(err) => resp(
                &format!("Cannot instantiate: {err}"),
                StatusCode::SERVICE_UNAVAILABLE,
            ),
            HandleRequestError::ErrorCode(code) => resp(
                &format!("Error code: {code}"),
                StatusCode::INTERNAL_SERVER_ERROR,
            ),
            HandleRequestError::ExecutionError(_) => {
                resp("Component Error", StatusCode::INTERNAL_SERVER_ERROR)
            }
            HandleRequestError::RouteNotFound => resp("Route not found", StatusCode::NOT_FOUND),
        })
    })
}

#[derive(Debug, thiserror::Error)]
pub enum HandleRequestError {
    #[error("incoming request error: {0}")]
    IncomingRequestError(StdError),
    #[error("response creation error: {0}")]
    ResponseCreationError(StdError),
    #[error("instantiation error: {0}")]
    InstantiationError(StdError),
    #[error("error code: {0}")]
    ErrorCode(wasmtime_wasi_http::bindings::http::types::ErrorCode),
    #[error("execution error: {0}")]
    ExecutionError(StdError),
    #[error("route not found")]
    RouteNotFound,
}

async fn handle_request_inner<
    C: ClockFn + 'static,
    DB: DbConnection + 'static,
    P: DbPool<DB> + 'static,
>(
    req: hyper::Request<hyper::body::Incoming>,
    router: Arc<MethodAwareRouter<WebhookInstance<C, DB, P>>>,
    engine: Arc<Engine>,
    clock_fn: C,
    db_pool: P,
    fn_registry: Arc<dyn FunctionRegistry>,
    retry_config: RetryConfig,
) -> Result<hyper::Response<HyperOutgoingBody>, HandleRequestError> {
    if let Some(matched) = router.find(req.method(), req.uri()) {
        let (sender, receiver) = tokio::sync::oneshot::channel();
        // if self.run.common.wasm.timeout.is_some() {
        //     store.set_epoch_deadline(u64::from(EPOCH_PRECISION) + 1);
        // }
        let mut store = WebhookCtx::new(
            &engine,
            clock_fn,
            db_pool,
            fn_registry,
            retry_config,
            matched.params().iter(),
        );
        let req = store
            .data_mut()
            .new_incoming_request(Scheme::Http, req)
            .map_err(|err| HandleRequestError::IncomingRequestError(err.into()))?;
        let out = store
            .data_mut()
            .new_response_outparam(sender)
            .map_err(|err| HandleRequestError::ResponseCreationError(err.into()))?;
        let proxy = matched
            .handler()
            .0
            .instantiate_async(&mut store)
            .await
            .map_err(|err| HandleRequestError::InstantiationError(err.into()))?;

        let task = tokio::task::spawn(async move {
            proxy
                .wasi_http_incoming_handler()
                .call_handle(store, req, out)
                .await
                .inspect_err(|err| error!("Webhook instance returned error: {err:?}"))
        });
        match receiver.await {
            Ok(Ok(resp)) => Ok(resp),
            Ok(Err(err)) => {
                error!("Webhook instance sent error code {err:?}");
                Err(HandleRequestError::ErrorCode(err))
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
                    Ok(r) => {
                        r.expect_err("if the receiver has an error, the task must have failed")
                    } //
                    Err(e) => e.into(), // e.g. Panic
                };
                info!("Setting response to ExecutionError");
                Err(HandleRequestError::ExecutionError(err.into()))
            }
        }
    } else {
        Err(HandleRequestError::RouteNotFound)
    }
}

#[cfg(test)]
mod tests {
    use super::MethodAwareRouter;
    use hyper::{Method, Uri};

    #[cfg(not(madsim))] // Due to TCP server/client
    mod nosim {
        use super::*;
        use crate::{
            activity_worker::tests::spawn_activity_fibo,
            tests::fn_registry_dummy,
            webhook_trigger,
            workflow_worker::{tests::spawn_workflow_fibo, JoinNextBlockingStrategy},
        };
        use concepts::FunctionFqn;
        use db_tests::Database;
        use std::net::SocketAddr;
        use test_programs_fibo_activity_builder::exports::testing::fibo::fibo::FIBO;
        use test_programs_fibo_workflow_builder::exports::testing::fibo_workflow::workflow::FIBOA;
        use test_utils::sim_clock::SimClock;
        use tokio::net::TcpListener;
        use tracing::info;
        use utils::wasm_tools::WasmComponent;

        struct AbortOnDrop<T>(tokio::task::JoinHandle<T>);
        impl<T> Drop for AbortOnDrop<T> {
            fn drop(&mut self) {
                self.0.abort();
            }
        }

        #[tokio::test]
        async fn webhook_trigger_fibo() {
            use crate::engines::{EngineConfig, Engines};

            test_utils::set_up();
            let addr = SocketAddr::from(([127, 0, 0, 1], 0));
            let sim_clock = SimClock::default();
            let (_guard, db_pool) = Database::Memory.set_up().await;
            let activity_exec_task = spawn_activity_fibo(db_pool.clone(), sim_clock.get_clock_fn());
            let fn_registry = fn_registry_dummy(&[
                FunctionFqn::new_static(FIBOA.0, FIBOA.1),
                FunctionFqn::new_static(FIBO.0, FIBO.1),
            ]);
            let engine = Engines::get_webhook_engine(EngineConfig::on_demand_testing()).unwrap();
            let workflow_exec_task = spawn_workflow_fibo(
                db_pool.clone(),
                sim_clock.get_clock_fn(),
                JoinNextBlockingStrategy::Await,
                0,
                fn_registry.clone(),
            );
            let instance = webhook_trigger::component_to_instance(
                &WasmComponent::new(
                    test_programs_webhook_trigger_fibo_builder::TEST_PROGRAMS_WEBHOOK_TRIGGER_FIBO,
                    &engine,
                )
                .unwrap(),
                &engine,
            )
            .unwrap();

            let mut router = MethodAwareRouter::default();
            router.add(Some(Method::GET), "/fibo/:N/:ITERATIONS", instance);
            let tcp_listener = TcpListener::bind(addr).await.unwrap();
            let server_addr = tcp_listener.local_addr().unwrap();
            info!("Listening on port {}", server_addr.port());

            let _server = AbortOnDrop(tokio::spawn(webhook_trigger::server(
                tcp_listener,
                engine,
                router,
                db_pool,
                sim_clock.get_clock_fn(),
                fn_registry,
                crate::webhook_trigger::RetryConfig::default(),
            )));
            // Check the happy path
            let resp = reqwest::get(format!("http://{}/fibo/1/1", &server_addr))
                .await
                .unwrap();
            assert_eq!(resp.status().as_u16(), 200);
            assert_eq!("fiboa(1, 1) = 1", resp.text().await.unwrap());
            // Check wrong URL
            let resp = reqwest::get(format!("http://{}/unknown", &server_addr))
                .await
                .unwrap();
            assert_eq!(resp.status().as_u16(), 404);
            assert_eq!("Route not found", resp.text().await.unwrap());
            // Check panicking inside WASM before response is streamed
            let resp = reqwest::get(format!("http://{}/fibo/1a/1", &server_addr))
                .await
                .unwrap();
            assert_eq!(resp.status().as_u16(), 500);
            assert_eq!("Component Error", resp.text().await.unwrap());
            // Check panicking inside WASM - AFTER response is streamed
            let resp = reqwest::get(format!("http://{}/fibo/1/1a", &server_addr))
                .await
                .unwrap();
            assert_eq!(resp.status().as_u16(), 200);
            assert_eq!("", resp.text().await.unwrap());
            activity_exec_task.close().await;
            workflow_exec_task.close().await;
        }
    }

    #[test]
    fn routes() {
        let mut router = MethodAwareRouter::default();
        router.add(Some(Method::GET), "/foo", 1);
        router.add(Some(Method::GET), "/foo/*", 2);
        router.add(None, "/foo", 3);
        router.add(None, "/*", 4);
        router.add(None, "/", 5);
        router.add(Some(Method::GET), "/path/:param1/:param2", 6);

        assert_eq!(
            1,
            **router
                .find(&Method::GET, &Uri::from_static("/foo"))
                .unwrap()
                .handler()
        );
        assert_eq!(
            2,
            **router
                .find(&Method::GET, &Uri::from_static("/foo/foo/"))
                .unwrap()
                .handler()
        );
        assert_eq!(
            2,
            **router
                .find(&Method::GET, &Uri::from_static("/foo/foo/bar"))
                .unwrap()
                .handler()
        );
        assert_eq!(
            3,
            **router
                .find(&Method::POST, &Uri::from_static("/foo"))
                .unwrap()
                .handler()
        );
        assert_eq!(
            5,
            **router
                .find(&Method::GET, &Uri::from_static("/"))
                .unwrap()
                .handler()
        );

        let found = router
            .find(&Method::GET, &Uri::from_static("/path/p1/p2"))
            .unwrap();
        assert_eq!(6, **found.handler());
        assert_eq!(
            hashbrown::HashMap::from([("param1", "p1"), ("param2", "p2")]),
            found
                .params()
                .into_iter()
                .collect::<hashbrown::HashMap<_, _>>()
        );
        let found = router
            .find(&Method::GET, &Uri::from_static("/path/p1/p2/p3"))
            .unwrap();
        assert_eq!(4, **found.handler());
    }

    #[test]
    fn routes_empty_fallback() {
        let mut router = MethodAwareRouter::default();
        router.add(Some(Method::GET), "/foo", 1);
        router.add(None, "", 9);

        assert_eq!(
            1,
            **router
                .find(&Method::GET, &Uri::from_static("/foo"))
                .unwrap()
                .handler()
        );
        assert_eq!(
            9,
            **router
                .find(&Method::GET, &Uri::from_static("/"))
                .unwrap()
                .handler()
        );
        assert_eq!(
            9,
            **router
                .find(&Method::GET, &Uri::from_static("/x"))
                .unwrap()
                .handler()
        );
        assert_eq!(
            9,
            **router
                .find(&Method::GET, &Uri::from_static("/x/"))
                .unwrap()
                .handler()
        );
    }
}
