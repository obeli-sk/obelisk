use crate::component_logger::{ComponentLogger, log_activities};
use crate::envvar::EnvVar;
use crate::std_output_stream::{LogStream, StdOutput};
use crate::workflow::host_exports::{SUFFIX_FN_SCHEDULE, history_event_schedule_at_from_wast_val};
use crate::{RunnableComponent, WasmFileError};
use assert_matches::assert_matches;
use concepts::prefixed_ulid::{ExecutionIdTopLevel, JOIN_SET_START_IDX};
use concepts::storage::{
    AppendRequest, BacktraceInfo, ClientError, CreateRequest, DbError, DbPool, ExecutionEventInner,
    HistoryEvent, JoinSetRequest, Version,
};
use concepts::time::ClockFn;
use concepts::{
    ClosingStrategy, ComponentId, ComponentType, ExecutionId, ExecutionMetadata,
    FinishedExecutionError, FunctionFqn, FunctionMetadata, FunctionRegistry, IfcFqnName,
    JoinSetKind, Params, PermanentFailureKind, ReturnType, SUFFIX_PKG_SCHEDULE,
    SUPPORTED_RETURN_VALUE_OK_EMPTY, StrVariant, TrapKind,
};
use concepts::{JoinSetId, SupportedFunctionReturnValue};
use http_body_util::combinators::BoxBody;
use hyper::body::Bytes;
use hyper::server::conn::http1;
use hyper::{Method, StatusCode, Uri};
use hyper_util::rt::TokioIo;
use route_recognizer::{Match, Router};
use std::ops::Deref;
use std::path::Path;
use std::time::Duration;
use std::{fmt::Debug, sync::Arc};
use tokio::net::TcpListener;
use tokio::sync::OwnedSemaphorePermit;
use tracing::{
    Instrument, Level, Span, debug, debug_span, error, info, info_span, instrument, trace,
};
use types_v3_0_0::obelisk::types::execution::Host as ExecutionHost;
use types_v3_0_0::obelisk::types::execution::HostJoinSet;
use utils::wasm_tools::ExIm;
use val_json::wast_val::WastVal;
use wasmtime::component::ResourceTable;
use wasmtime::component::{Linker, Val};
use wasmtime::{Engine, Store, UpdateDeadline};
use wasmtime_wasi::{WasiCtx, WasiCtxBuilder, WasiCtxView, WasiView};
use wasmtime_wasi_http::bindings::ProxyPre;
use wasmtime_wasi_http::bindings::http::types::Scheme;
use wasmtime_wasi_http::body::HyperOutgoingBody;
use wasmtime_wasi_http::{WasiHttpCtx, WasiHttpView};
use wasmtime_wasi_io::IoView;

const HTTP_HANDLER_FFQN: FunctionFqn =
    FunctionFqn::new_static("wasi:http/incoming-handler", "handle");

pub(crate) mod types_v3_0_0 {
    wasmtime::component::bindgen!({
        path: "host-wit-webhook/",
        inline: "package any:any;
                world bindings {
                    import obelisk:types/time@3.0.0;
                    import obelisk:types/execution@3.0.0;
                }",
        world: "any:any/bindings",
        exports: {
            default: trappable | async,
        },
        with: {
            "obelisk:types/execution/join-set": concepts::JoinSetId,
        }
    });
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct HttpTriggerConfig {
    pub component_id: ComponentId,
}
type StdError = Box<dyn std::error::Error + Send + Sync>;

#[derive(Debug, thiserror::Error)]
pub enum WebhookServerError {
    #[error("socket error: {0}")]
    SocketError(std::io::Error),
}

pub struct WebhookEndpointCompiled {
    pub config: WebhookEndpointConfig,
    pub runnable_component: RunnableComponent,
}

impl WebhookEndpointCompiled {
    pub fn new(
        config: WebhookEndpointConfig,
        wasm_path: impl AsRef<Path>,
        engine: &Engine,
    ) -> Result<Self, WasmFileError> {
        let runnable_component =
            RunnableComponent::new(wasm_path, engine, ComponentType::WebhookEndpoint)?;
        Ok(Self {
            config,
            runnable_component,
        })
    }

    #[must_use]
    pub fn imports(&self) -> &[FunctionMetadata] {
        &self.runnable_component.wasm_component.exim.imports_flat
    }

    #[instrument(skip_all, fields(component_id = %self.config.component_id), err)]
    pub fn link<C: ClockFn>(
        self,
        engine: &Engine,
        fn_registry: &dyn FunctionRegistry,
    ) -> Result<WebhookEndpointInstance<C>, WasmFileError> {
        let mut linker = Linker::new(engine);
        // Link wasi
        wasmtime_wasi::p2::add_to_linker_async(&mut linker).map_err(|err| {
            WasmFileError::LinkingError {
                context: StrVariant::Static("linking `wasmtime_wasi`"),
                err: err.into(),
            }
        })?;
        // Link wasi-http
        wasmtime_wasi_http::add_only_http_to_linker_async(&mut linker).map_err(|err| {
            WasmFileError::LinkingError {
                context: StrVariant::Static("linking `wasmtime_wasi_http`"),
                err: err.into(),
            }
        })?;
        // Link log and types
        WebhookEndpointCtx::add_to_linker(&mut linker)?;

        // Mock imported functions
        for import in fn_registry
            .all_exports()
            .iter()
            .filter(|import| {
                // Skip already linked functions to avoid unexpected behavior and security issues.
                !import.ifc_fqn.is_namespace_obelisk() && !import.ifc_fqn.is_namespace_wasi()
            })
            .filter(|import| {
                // Keep only no-ext and -schedule interfaces
                !import.ifc_fqn.is_extension()
                    || import
                        .ifc_fqn
                        .package_strip_obelisk_schedule_suffix()
                        .is_some()
            })
        {
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
                        move |mut store_ctx: wasmtime::StoreContextMut<
                            '_,
                            WebhookEndpointCtx<C>,
                        >,
                              params: &[Val],
                              results: &mut [Val]| {
                            let ffqn = ffqn.clone();
                            let wasm_backtrace = if self.config.backtrace_persist {
                                let wasm_backtrace = wasmtime::WasmBacktrace::capture(&store_ctx);
                                concepts::storage::WasmBacktrace::maybe_from(&wasm_backtrace)
                            } else {
                                None
                            };

                            Box::new(async move {
                                Ok(store_ctx
                                    .data_mut()
                                    .call_imported_fn(ffqn, params, results, wasm_backtrace)
                                    .await?)
                            })
                        }
                    });
                    if let Err(err) = res {
                        return Err(WasmFileError::LinkingError {
                            context: StrVariant::Arc(Arc::from(format!(
                                "cannot add mock for imported function {ffqn}"
                            ))),
                            err: err.into(),
                        });
                    }
                }
            } else {
                trace!("Skipping interface {ifc_fqn}", ifc_fqn = import.ifc_fqn);
            }
        }

        // Pre-instantiate to catch missing imports
        let proxy_pre = linker
            .instantiate_pre(&self.runnable_component.wasmtime_component)
            .map_err(|err: wasmtime::Error| WasmFileError::LinkingError {
                context: StrVariant::Static("linking error while creating instantiate_pre"),
                err: err.into(),
            })?;
        let proxy_pre = Arc::new(ProxyPre::new(proxy_pre).map_err(|err: wasmtime::Error| {
            WasmFileError::LinkingError {
                context: StrVariant::Static("linking error while creating ProxyPre instance"),
                err: err.into(),
            }
        })?);

        Ok(WebhookEndpointInstance {
            config: self.config,
            exim: self.runnable_component.wasm_component.exim,
            proxy_pre,
        })
    }
}

#[derive(Clone, derive_more::Debug)]
pub struct WebhookEndpointInstance<C: ClockFn> {
    #[debug(skip)]
    proxy_pre: Arc<ProxyPre<WebhookEndpointCtx<C>>>,
    pub config: WebhookEndpointConfig,
    exim: ExIm,
}

impl<C: ClockFn> WebhookEndpointInstance<C> {
    #[must_use]
    pub fn imported_functions(&self) -> &[FunctionMetadata] {
        &self.exim.imports_flat
    }
}

pub struct MethodAwareRouter<T> {
    method_map: hashbrown::HashMap<Method, Router<T>>,
    fallback: Router<T>, // Routes that do not specify a method. Will be queried only if no match is found in `method_map`.
}

// Clone only because of potentially registering 2 paths via `route-recognizer`
impl<T: Clone> MethodAwareRouter<T> {
    pub fn add(&mut self, method: Option<Method>, route: &str, dest: T) {
        let route = if route.is_empty() { "/*" } else { route };

        let mut add = |method, route, dest| {
            if let Some(method) = method {
                self.method_map.entry(method).or_default().add(route, dest);
            } else {
                self.fallback.add(route, dest);
            }
        };

        let prefix_with_slash;
        if let Some(prefix) = route.strip_suffix("/*") {
            // Add {prefix}/ because the library would not match it otherwise.
            prefix_with_slash = format!("{prefix}/");
            add(method.clone(), &prefix_with_slash, dest.clone());
        }
        add(method, route, dest);
    }
}

impl<T> MethodAwareRouter<T> {
    fn find(&self, method: &Method, path: &Uri) -> Option<Match<&T>> {
        let path = path.path();
        self.method_map
            .get(method)
            .and_then(|router| router.recognize(path).ok())
            .or_else(|| self.fallback.recognize(path).ok())
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

#[expect(clippy::too_many_arguments)]
pub async fn server<C: ClockFn + 'static>(
    http_server: StrVariant,
    listener: TcpListener,
    engine: Arc<Engine>,
    router: MethodAwareRouter<WebhookEndpointInstance<C>>,
    db_pool: Arc<dyn DbPool>,
    clock_fn: C,
    fn_registry: Arc<dyn FunctionRegistry>,
    max_inflight_requests: Option<Arc<tokio::sync::Semaphore>>,
) -> Result<(), WebhookServerError> {
    let router = Arc::new(router);
    loop {
        let (stream, _) = listener
            .accept()
            .await
            .map_err(WebhookServerError::SocketError)?;
        let stream = TokioIo::new(stream);

        // Spawn a tokio task for each TCP stream.
        // TODO: cancel on connection drop and on server exit
        tokio::task::spawn({
                let router = router.clone();
                let engine = engine.clone();
                let clock_fn = clock_fn.clone();
                let db_pool = db_pool.clone();
                let fn_registry = fn_registry.clone();
                let http_server = http_server.clone();
                let connection_span = info_span!("webhook_endpoint", %http_server);
                let max_inflight_requests = max_inflight_requests.clone();
                async move {
                    let res = http1::Builder::new()
                        .serve_connection(
                            stream,
                            hyper::service::service_fn(move |req| {
                                let execution_id = ExecutionId::generate().get_top_level();
                                debug!(%execution_id, method = %req.method(), uri = %req.uri(), "Processing request");
                                RequestHandler {
                                    engine: engine.clone(),
                                    clock_fn: clock_fn.clone(),
                                    db_pool: db_pool.clone(),
                                    fn_registry: fn_registry.clone(),
                                    execution_id,
                                    router: router.clone(),
                                }
                                .handle_request(req, max_inflight_requests.clone())
                                }.instrument(info_span!(parent: &connection_span, "Connection"))
                            )
                        )
                        .await;
                    if let Err(err) = res {
                        info!(%http_server, "Error serving connection: {err:?}");
                    }
                }
                }.instrument(debug_span!("tcp stream")));
    }
}

#[derive(Debug, Clone)]
pub struct WebhookEndpointConfig {
    pub component_id: ComponentId,
    pub forward_stdout: Option<StdOutput>,
    pub forward_stderr: Option<StdOutput>,
    pub env_vars: Arc<[EnvVar]>,
    pub fuel: Option<u64>,
    pub backtrace_persist: bool,
}

struct WebhookEndpointCtx<C: ClockFn> {
    component_id: ComponentId,
    clock_fn: C,
    db_pool: Arc<dyn DbPool>,
    fn_registry: Arc<dyn FunctionRegistry>,
    table: ResourceTable,
    wasi_ctx: WasiCtx,
    http_ctx: WasiHttpCtx,
    execution_id: ExecutionIdTopLevel,
    next_join_set_idx: u64,
    version: Option<Version>,
    component_logger: ComponentLogger,
}

impl<C: ClockFn> HostJoinSet for WebhookEndpointCtx<C> {
    fn id(&mut self, _resource: wasmtime::component::Resource<JoinSetId>) -> String {
        unreachable!("webhook endpoint instances cannot obtain `join-set-id` resource")
    }

    fn drop(
        &mut self,
        _resource: wasmtime::component::Resource<JoinSetId>,
    ) -> wasmtime::Result<()> {
        unreachable!("webhook endpoint instances cannot obtain `join-set-id` resource")
    }
}

impl<C: ClockFn> ExecutionHost for WebhookEndpointCtx<C> {}

#[derive(thiserror::Error, Debug, Clone)]
#[expect(clippy::enum_variant_names)]
enum WebhookEndpointFunctionError {
    #[error(transparent)]
    DbError(#[from] DbError),
    #[error(transparent)]
    FinishedExecutionError(#[from] FinishedExecutionError),
    #[error("uncategorized error: {0}")]
    UncategorizedError(&'static str),
}

impl<C: ClockFn> wasmtime::component::HasData for WebhookEndpointCtx<C> {
    type Data<'a> = &'a mut WebhookEndpointCtx<C>;
}

impl<C: ClockFn> WebhookEndpointCtx<C> {
    // Create new execution if this is the first call of the request/response cycle
    async fn get_version(&mut self) -> Result<Version, DbError> {
        if let Some(found) = &self.version {
            return Ok(found.clone());
        }
        let created_at = self.clock_fn.now();
        // Associate the top level execution with the request span. Allows to find the trace by execution id.
        let metadata = concepts::ExecutionMetadata::from_parent_span(&self.component_logger.span);
        let create_request = CreateRequest {
            created_at,
            execution_id: ExecutionId::TopLevel(self.execution_id),
            ffqn: HTTP_HANDLER_FFQN,
            params: Params::empty(),
            parent: None,
            metadata,
            scheduled_at: created_at,
            max_retries: 0,
            retry_exp_backoff: Duration::ZERO,
            component_id: self.component_id.clone(),
            scheduled_by: None,
        };
        let conn = self.db_pool.connection();
        let version = conn.create(create_request).await?;
        self.version = Some(version.clone());
        Ok(version)
    }

    #[instrument(level = Level::DEBUG, skip_all, fields(%ffqn, version), err)]
    async fn call_imported_fn(
        &mut self,
        ffqn: FunctionFqn,
        params: &[Val],
        results: &mut [Val],
        wasm_backtrace: Option<concepts::storage::WasmBacktrace>,
    ) -> Result<(), WebhookEndpointFunctionError> {
        debug!(?params, "call_imported_fn start");
        assert_eq!(
            1,
            results.len(),
            "direct call: no-ext export must return `result`, -schedule returns `execuiton-id`"
        );
        let (db_connection, version_min_including, version_max_excluding) = if let Some(
            package_name,
        ) =
            ffqn.ifc_fqn.package_strip_obelisk_schedule_suffix()
        {
            // -schedule
            let ifc_fqn = IfcFqnName::from_parts(
                ffqn.ifc_fqn.namespace(),
                package_name,
                ffqn.ifc_fqn.ifc_name(),
                ffqn.ifc_fqn.version(),
            );
            if let Some(function_name) = ffqn.function_name.strip_suffix(SUFFIX_FN_SCHEDULE) {
                let ffqn =
                    FunctionFqn::new_arc(Arc::from(ifc_fqn.to_string()), Arc::from(function_name));
                debug!("Got `-schedule` extension for {ffqn}");
                let Some((schedule_at, params)) = params.split_first() else {
                    error!(
                        "Error running `-schedule` extension function: exepcted at least one parameter of type `schedule-at`, got empty parameter list"
                    );
                    return Err(WebhookEndpointFunctionError::UncategorizedError(
                        "error running `-schedule` extension function: exepcted at least one parameter of type `schedule-at`, got empty parameter list",
                    ));
                };
                let schedule_at =
                    WastVal::try_from(schedule_at.clone()).map_err(|err| {
                        error!("Error running `-schedule` extension function: cannot convert to internal representation - {err:?}");
                        WebhookEndpointFunctionError::UncategorizedError(
                            "error running `-schedule` extension function: cannot convert to internal representation",
                        )
                    })?;
                let schedule_at = match history_event_schedule_at_from_wast_val(&schedule_at) {
                    Ok(ok) => ok,
                    Err(err) => {
                        error!(
                            "Wrong type for the first `-schedule` extension function parameter, expected `schedule-at`, got `{schedule_at:?}` - {err:?}"
                        );
                        return Err(WebhookEndpointFunctionError::UncategorizedError(
                            "error running `-schedule` extension function: wrong first parameter type",
                        ));
                    }
                };
                // Write to db
                let version = self.get_version().await?;
                let span = Span::current();
                span.record("version", tracing::field::display(&version));
                let new_execution_id = ExecutionId::generate();
                let (_function_metadata, component_id, import_retry_config) = self
                    .fn_registry
                    .get_by_exported_function(&ffqn)
                    .expect("target function must be found in fn_registry");
                let created_at = self.clock_fn.now();

                let event = HistoryEvent::Schedule {
                    execution_id: new_execution_id.clone(),
                    schedule_at,
                };
                let schedule_at = schedule_at.as_date_time(created_at).map_err(|_err| {
                    WebhookEndpointFunctionError::UncategorizedError("schedule-at conversion error")
                })?;
                let child_exec_req = AppendRequest {
                    event: ExecutionEventInner::HistoryEvent { event },
                    created_at,
                };

                let create_child_req = CreateRequest {
                    created_at,
                    execution_id: new_execution_id.clone(),
                    ffqn,
                    params: Params::from_wasmtime(Arc::from(params)),
                    parent: None, // Schedule breaks from the parent-child relationship to avoid a linked list
                    metadata: ExecutionMetadata::from_linked_span(&self.component_logger.span),
                    scheduled_at: schedule_at,
                    max_retries: import_retry_config.max_retries,
                    retry_exp_backoff: import_retry_config.retry_exp_backoff,
                    component_id,
                    scheduled_by: Some(ExecutionId::TopLevel(self.execution_id)),
                };
                let db_connection = self.db_pool.connection();
                let version_min_including = version.0;
                let version = db_connection
                    .append_batch_create_new_execution(
                        created_at,
                        vec![child_exec_req],
                        ExecutionId::TopLevel(self.execution_id),
                        version.clone(),
                        vec![create_child_req],
                    )
                    .await?;
                self.version = Some(version.clone());
                results[0] = execution_id_into_val(&new_execution_id);
                (db_connection, version_min_including, version.0)
            } else {
                error!("unrecognized `{SUFFIX_PKG_SCHEDULE}` extension function {ffqn}");
                return Err(WebhookEndpointFunctionError::UncategorizedError(
                    "unrecognized extension function",
                ));
            }
        } else {
            // direct call
            let version = self.get_version().await?;
            let span = Span::current();
            span.record("version", tracing::field::display(&version));
            let join_set_id_direct = JoinSetId::new(
                JoinSetKind::OneOff,
                StrVariant::from(self.next_join_set_idx.to_string()),
            )
            .expect("numeric names must be allowed");
            self.next_join_set_idx += 1;
            // Create oneoff execution id: next_join_set_idx_1
            let child_execution_id =
                ExecutionId::TopLevel(self.execution_id).next_level(&join_set_id_direct);
            let created_at = self.clock_fn.now();
            let (fn_metadata, component_id, import_retry_config) = self
                .fn_registry
                .get_by_exported_function(&ffqn)
                .expect("import was mocked using fn_registry exports limited to -schedule and no-ext functions");
            assert!(
                fn_metadata.extension.is_none(),
                "direct call: function must be no-ext"
            );
            let return_type_tl = assert_matches!(fn_metadata.return_type, ReturnType::Extendable(compatible) => compatible.type_wrapper_tl);

            let req_join_set_created = AppendRequest {
                created_at,
                event: ExecutionEventInner::HistoryEvent {
                    event: HistoryEvent::JoinSetCreate {
                        join_set_id: join_set_id_direct.clone(),
                        closing_strategy: ClosingStrategy::Complete,
                    },
                },
            };
            let req_child_exec = AppendRequest {
                created_at,
                event: ExecutionEventInner::HistoryEvent {
                    event: HistoryEvent::JoinSetRequest {
                        join_set_id: join_set_id_direct.clone(),
                        request: JoinSetRequest::ChildExecutionRequest {
                            child_execution_id: child_execution_id.clone(),
                        },
                    },
                },
            };
            let req_join_next = AppendRequest {
                created_at,
                event: ExecutionEventInner::HistoryEvent {
                    event: HistoryEvent::JoinNext {
                        join_set_id: join_set_id_direct.clone(),
                        run_expires_at: created_at, // does not matter what the pending state is.
                        closing: false,
                        requested_ffqn: Some(ffqn.clone()), // only needed for workflows but added for consistency.
                    },
                },
            };
            let req_create_child = CreateRequest {
                created_at,
                execution_id: ExecutionId::Derived(child_execution_id.clone()),
                ffqn: ffqn.clone(),
                params: Params::from_wasmtime(Arc::from(params)),
                parent: Some((ExecutionId::TopLevel(self.execution_id), join_set_id_direct)),
                metadata: ExecutionMetadata::from_parent_span(&self.component_logger.span),
                scheduled_at: created_at,
                max_retries: import_retry_config.max_retries,
                retry_exp_backoff: import_retry_config.retry_exp_backoff,
                component_id,
                scheduled_by: None,
            };
            let db_connection = self.db_pool.connection();
            let version_min_including = version.0;
            let appended = vec![req_join_set_created, req_child_exec, req_join_next];
            let version_max_excluding = version_min_including + 3; // obvious from line above
            let version = db_connection
                .append_batch_create_new_execution(
                    created_at,
                    appended,
                    ExecutionId::TopLevel(self.execution_id),
                    version,
                    vec![req_create_child],
                )
                .await?;
            self.version = Some(version);

            let res = db_connection
                .wait_for_finished_result(
                    &ExecutionId::Derived(child_execution_id),
                    None, /* TODO timeouts */
                )
                .await;
            trace!("Finished result: {res:?}");

            let res = match res {
                Ok(res) => res,
                Err(ClientError::DbError(err)) => {
                    return Err(WebhookEndpointFunctionError::DbError(err));
                }
                Err(ClientError::Timeout) => unreachable!("timeout was not set"),
            };

            results[0] = res.into_wast_val(move || return_type_tl).as_val();

            trace!(?params, ?results, "call_imported_fn finish");
            (db_connection, version_min_including, version_max_excluding)
        };

        if let Some(wasm_backtrace) = wasm_backtrace
            && let Err(err) = db_connection
                .append_backtrace(BacktraceInfo {
                    execution_id: ExecutionId::TopLevel(self.execution_id),
                    component_id: self.component_id.clone(),
                    version_min_including: Version::new(version_min_including),
                    version_max_excluding: Version::new(version_max_excluding),
                    wasm_backtrace,
                })
                .await
        {
            debug!("Ignoring error while appending backtrace: {err:?}");
        }
        Ok(())
    }

    fn add_to_linker(linker: &mut Linker<WebhookEndpointCtx<C>>) -> Result<(), WasmFileError> {
        // link obelisk:log
        log_activities::obelisk::log::log::add_to_linker::<_, WebhookEndpointCtx<C>>(linker, |x| x)
            .map_err(|err| WasmFileError::LinkingError {
                context: StrVariant::Static("linking log activities"),
                err: err.into(),
            })?;
        // link obelisk:types
        types_v3_0_0::obelisk::types::execution::add_to_linker::<_, WebhookEndpointCtx<C>>(
            linker,
            |x| x,
        )
        .map_err(|err| WasmFileError::LinkingError {
            context: StrVariant::Static("linking obelisk:types"),
            err: err.into(),
        })?;
        Ok(())
    }

    #[must_use]
    #[expect(clippy::too_many_arguments)]
    fn new<'a>(
        config: WebhookEndpointConfig,
        engine: &Engine,
        clock_fn: C,
        db_pool: Arc<dyn DbPool>,
        fn_registry: Arc<dyn FunctionRegistry>,
        params: impl Iterator<Item = (&'a str, &'a str)>,
        execution_id: ExecutionIdTopLevel,
        request_span: Span,
    ) -> Store<WebhookEndpointCtx<C>> {
        let mut wasi_ctx = WasiCtxBuilder::new();
        if let Some(stdout) = config.forward_stdout {
            let stdout = LogStream::new(
                format!(
                    "[{component_id} {execution_id} stdout]",
                    component_id = config.component_id
                ),
                stdout,
            );
            wasi_ctx.stdout(stdout);
        }
        if let Some(stderr) = config.forward_stderr {
            let stderr = LogStream::new(
                format!(
                    "[{component_id} {execution_id} stderr]",
                    component_id = config.component_id
                ),
                stderr,
            );
            wasi_ctx.stderr(stderr);
        }
        for env_var in config.env_vars.as_ref() {
            wasi_ctx.env(&env_var.key, &env_var.val);
        }
        for (key, val) in params {
            wasi_ctx.env(key, val);
        }
        let wasi_ctx = wasi_ctx.build();
        // All child executions are part of the same join set.
        let ctx = WebhookEndpointCtx {
            clock_fn,
            db_pool,
            fn_registry,
            table: ResourceTable::new(),
            wasi_ctx,
            http_ctx: WasiHttpCtx::new(),
            version: None,
            component_id: config.component_id,
            next_join_set_idx: JOIN_SET_START_IDX,
            execution_id,
            component_logger: ComponentLogger { span: request_span },
        };
        let mut store = Store::new(engine, ctx);

        // Set fuel.
        if let Some(fuel) = config.fuel {
            store
                .set_fuel(fuel)
                .expect("engine must have `consume_fuel` enabled");
        }

        // Configure epoch callback before running the initialization to avoid interruption
        store.epoch_deadline_callback(|_store_ctx| Ok(UpdateDeadline::Yield(1)));
        store
    }

    async fn close(
        self,
        original_result: wasmtime::Result<()>,
        assigned_fuel: Option<u64>,
    ) -> wasmtime::Result<()> {
        #[derive(Debug, thiserror::Error)]
        #[error("webhook {trap_kind}: {reason}")]
        struct WebhookTrap {
            reason: String,
            trap_kind: TrapKind,
            detail: Option<String>,
        }

        let result = match &original_result {
            Ok(()) => SUPPORTED_RETURN_VALUE_OK_EMPTY,
            Err(err) => {
                let err = if let Some(trap) = err
                    .source()
                    .and_then(|source| source.downcast_ref::<wasmtime::Trap>())
                {
                    if *trap == wasmtime::Trap::OutOfFuel {
                        WebhookTrap {
                            reason: format!(
                                "total fuel consumed: {}",
                                assigned_fuel
                                    .expect("must have been set as it was the reason of trap")
                            ),
                            detail: None,
                            trap_kind: TrapKind::OutOfFuel,
                        }
                    } else {
                        WebhookTrap {
                            reason: trap.to_string(),
                            detail: Some(format!("{err:?}")),
                            trap_kind: TrapKind::Trap,
                        }
                    }
                } else {
                    WebhookTrap {
                        reason: err.to_string(),
                        trap_kind: TrapKind::HostFunctionError,
                        detail: Some(format!("{err:?}")),
                    }
                };

                SupportedFunctionReturnValue::ExecutionError(
                    FinishedExecutionError::PermanentFailure {
                        reason_full: err.to_string(),
                        reason_inner: err.reason,
                        kind: PermanentFailureKind::WebhookEndpointError,
                        detail: err.detail,
                    },
                )
            }
        };
        if let Some(version) = self.version {
            self.db_pool
                .connection()
                .append(
                    ExecutionId::TopLevel(self.execution_id),
                    version,
                    AppendRequest {
                        created_at: self.clock_fn.now(),
                        event: ExecutionEventInner::Finished {
                            result,
                            http_client_traces: None,
                        },
                    },
                )
                .await?;
        }
        original_result
    }
}

impl<C: ClockFn> log_activities::obelisk::log::log::Host for WebhookEndpointCtx<C> {
    fn trace(&mut self, message: String) {
        self.component_logger.trace(&message);
    }

    fn debug(&mut self, message: String) {
        self.component_logger.debug(&message);
    }

    fn info(&mut self, message: String) {
        self.component_logger.info(&message);
    }

    fn warn(&mut self, message: String) {
        self.component_logger.warn(&message);
    }

    fn error(&mut self, message: String) {
        self.component_logger.error(&message);
    }
}

impl<C: ClockFn> WasiView for WebhookEndpointCtx<C> {
    fn ctx(&mut self) -> WasiCtxView<'_> {
        WasiCtxView {
            ctx: &mut self.wasi_ctx,
            table: &mut self.table,
        }
    }
}
impl<C: ClockFn> IoView for WebhookEndpointCtx<C> {
    fn table(&mut self) -> &mut ResourceTable {
        &mut self.table
    }
}
impl<C: ClockFn> WasiHttpView for WebhookEndpointCtx<C> {
    fn ctx(&mut self) -> &mut WasiHttpCtx {
        &mut self.http_ctx
    }

    fn table(&mut self) -> &mut ResourceTable {
        &mut self.table
    }
}

struct RequestHandler<C: ClockFn + 'static> {
    engine: Arc<Engine>,
    clock_fn: C,
    db_pool: Arc<dyn DbPool>,
    fn_registry: Arc<dyn FunctionRegistry>,
    execution_id: ExecutionIdTopLevel,
    router: Arc<MethodAwareRouter<WebhookEndpointInstance<C>>>,
}

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

impl<C: ClockFn + 'static> RequestHandler<C> {
    #[instrument(skip_all, name="incoming webhook request", fields(execution_id = %self.execution_id))]
    async fn handle_request(
        self,
        req: hyper::Request<hyper::body::Incoming>,
        max_inflight_requests: Option<Arc<tokio::sync::Semaphore>>,
    ) -> Result<hyper::Response<HyperOutgoingBody>, hyper::Error> {
        let http_request_guard = if let Some(http_request_semaphore) = &max_inflight_requests {
            http_request_semaphore.clone().try_acquire_owned().map(Some)
        } else {
            Ok(None)
        };
        let Ok(http_request_guard) = http_request_guard else {
            debug!(method = %req.method(), uri = %req.uri(), "Too many requests");
            return Ok::<_, hyper::Error>(resp("Out of permits", StatusCode::TOO_MANY_REQUESTS));
        };

        self.handle_request_inner(req, http_request_guard, Span::current())
            .await
            .or_else(|err| {
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
                    HandleRequestError::RouteNotFound => {
                        resp("Route not found", StatusCode::NOT_FOUND)
                    }
                    HandleRequestError::Timeout => resp("Timeout", StatusCode::REQUEST_TIMEOUT),
                    HandleRequestError::InstanceLimitReached => {
                        resp("Instance limit reached", StatusCode::SERVICE_UNAVAILABLE)
                    }
                })
            })
    }

    async fn handle_request_inner(
        self,
        req: hyper::Request<hyper::body::Incoming>,
        http_request_guard: Option<OwnedSemaphorePermit>,
        request_span: Span,
    ) -> Result<hyper::Response<HyperOutgoingBody>, HandleRequestError> {
        #[derive(Debug, thiserror::Error)]
        #[error("timeout")]
        struct TimeoutError;

        if let Some(matched) = self.router.find(req.method(), req.uri()) {
            let found_instance = matched.handler();
            let (sender, receiver) = tokio::sync::oneshot::channel();
            let mut store = WebhookEndpointCtx::new(
                found_instance.config.clone(),
                &self.engine,
                self.clock_fn,
                self.db_pool,
                self.fn_registry,
                matched.params().iter(),
                self.execution_id,
                request_span.clone(),
            );
            let req = store
                .data_mut()
                .new_incoming_request(Scheme::Http, req)
                .map_err(|err| HandleRequestError::IncomingRequestError(err.into()))?;
            let out = store
                .data_mut()
                .new_response_outparam(sender)
                .map_err(|err| HandleRequestError::ResponseCreationError(err.into()))?;
            let proxy = found_instance
                .proxy_pre
                .instantiate_async(&mut store)
                .await
                .map_err(|err| HandleRequestError::InstantiationError(err.into()))?;

            let task = tokio::task::spawn({
                let assigned_fuel = found_instance.config.fuel;
                async move {
                    let _http_request_guard = http_request_guard;
                    let result = proxy
                        .wasi_http_incoming_handler()
                        .call_handle(&mut store, req, out)
                        .await
                        .inspect_err(|err| error!("Webhook instance returned error: {err:?}"));
                    let ctx = store.into_data();
                    ctx.close(result, assigned_fuel).await
                }
                .instrument(request_span)
            });
            match receiver.await {
                Ok(Ok(resp)) => {
                    debug!("Streaming the response");
                    Ok(resp)
                }
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
                    if err.downcast_ref::<TimeoutError>().is_some() {
                        Err(HandleRequestError::Timeout)
                    } else {
                        info!("Webhook task ended with ExecutionError - {err:?}");
                        Err(HandleRequestError::ExecutionError(err.into()))
                    }
                }
            }
        } else {
            Err(HandleRequestError::RouteNotFound)
        }
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
    #[error("error code: {0}")]
    ErrorCode(wasmtime_wasi_http::bindings::http::types::ErrorCode),
    #[error("execution error: {0}")]
    ExecutionError(StdError),
    #[error("route not found")]
    RouteNotFound,
    #[error("instance limit reached")]
    InstanceLimitReached,
    #[error("timeout")]
    Timeout,
}

fn execution_id_into_val(execution_id: &ExecutionId) -> Val {
    Val::Record(vec![(
        "id".to_string(),
        Val::String(execution_id.to_string()),
    )])
}

#[cfg(test)]
pub(crate) mod tests {
    use super::MethodAwareRouter;
    use hyper::{Method, Uri};

    pub(crate) mod nosim {
        use super::*;
        use crate::RunnableComponent;
        use crate::activity::activity_worker::tests::{FIBO_10_OUTPUT, compile_activity};
        use crate::engines::{EngineConfig, Engines};
        use crate::testing_fn_registry::TestingFnRegistry;
        use crate::webhook::webhook_trigger::{
            self, WebhookEndpointCompiled, WebhookEndpointConfig,
        };
        use crate::workflow::workflow_worker::tests::compile_workflow;
        use crate::{
            activity::activity_worker::tests::spawn_activity_fibo,
            workflow::workflow_worker::{JoinNextBlockingStrategy, tests::spawn_workflow_fibo},
        };
        use assert_matches::assert_matches;
        use concepts::time::TokioSleep;
        use concepts::{ComponentId, ComponentType, StrVariant, SupportedFunctionReturnValue};
        use concepts::{ExecutionId, storage::DbPool};
        use db_tests::{Database, DbGuard};
        use executor::executor::ExecutorTaskHandle;
        use std::net::SocketAddr;
        use std::str::FromStr;
        use std::sync::Arc;
        use test_utils::sim_clock::SimClock;
        use tokio::net::TcpListener;
        use tracing::info;
        use val_json::type_wrapper::TypeWrapper;
        use val_json::wast_val::{WastVal, WastValWithType};

        struct AbortOnDrop(tokio::task::AbortHandle);
        impl Drop for AbortOnDrop {
            fn drop(&mut self) {
                self.0.abort();
            }
        }

        pub(crate) fn compile_webhook(wasm_path: &str) -> RunnableComponent {
            let engine = Engines::get_webhook_engine(EngineConfig::on_demand_testing()).unwrap();
            RunnableComponent::new(wasm_path, &engine, ComponentType::WebhookEndpoint).unwrap()
        }

        struct SetUpFiboWebhook {
            _server: AbortOnDrop,
            #[expect(dead_code)]
            guard: DbGuard,
            db_pool: Arc<dyn DbPool>,
            server_addr: SocketAddr,
            activity_exec_task: ExecutorTaskHandle,
            workflow_exec_task: ExecutorTaskHandle,
        }

        impl SetUpFiboWebhook {
            async fn new() -> Self {
                let addr = SocketAddr::from(([127, 0, 0, 1], 0));
                let sim_clock = SimClock::default();
                let (guard, db_pool) = Database::Memory.set_up().await;
                let activity_exec_task =
                    spawn_activity_fibo(db_pool.clone(), sim_clock.clone(), TokioSleep);
                let fn_registry = TestingFnRegistry::new_from_components(vec![
                    compile_activity(
                        test_programs_fibo_activity_builder::TEST_PROGRAMS_FIBO_ACTIVITY,
                    ),
                    compile_workflow(
                        test_programs_fibo_workflow_builder::TEST_PROGRAMS_FIBO_WORKFLOW,
                    ),
                ]);
                let engine =
                    Engines::get_webhook_engine(EngineConfig::on_demand_testing()).unwrap();
                let workflow_exec_task = spawn_workflow_fibo(
                    db_pool.clone(),
                    sim_clock.clone(),
                    JoinNextBlockingStrategy::Await {
                        non_blocking_event_batching: 0,
                    },
                    &fn_registry,
                );

                let router = {
                    let instance = WebhookEndpointCompiled::new(
                        WebhookEndpointConfig {
                            component_id: ComponentId::dummy_activity(),
                            forward_stdout: None,
                            forward_stderr: None,
                            env_vars: Arc::from([]),
                            fuel: None,
                            backtrace_persist: false,
                        },
                        test_programs_fibo_webhook_builder::TEST_PROGRAMS_FIBO_WEBHOOK,
                        &engine,
                    )
                    .unwrap()
                    .link(&engine, fn_registry.as_ref())
                    .unwrap();
                    let mut router = MethodAwareRouter::default();
                    router.add(Some(Method::GET), "/fibo/:N/:ITERATIONS", instance);
                    router
                };
                let tcp_listener = TcpListener::bind(addr).await.unwrap();
                let server_addr = tcp_listener.local_addr().unwrap();
                info!("Listening on port {}", server_addr.port());

                let server = AbortOnDrop(
                    tokio::spawn(webhook_trigger::server(
                        StrVariant::Static("test"),
                        tcp_listener,
                        engine,
                        router,
                        db_pool.clone(),
                        sim_clock.clone(),
                        fn_registry,
                        None,
                    ))
                    .abort_handle(),
                );
                Self {
                    _server: server,
                    server_addr,
                    activity_exec_task,
                    workflow_exec_task,
                    guard,
                    db_pool,
                }
            }

            async fn fetch(&self, n: u8, iterations: u32, expected_status_code: u16) -> String {
                let resp = reqwest::get(format!(
                    "http://{}/fibo/{n}/{iterations}",
                    &self.server_addr
                ))
                .await
                .unwrap();
                assert_eq!(resp.status().as_u16(), expected_status_code);
                resp.text().await.unwrap()
            }

            async fn close(self) {
                self.activity_exec_task.close().await;
                self.workflow_exec_task.close().await;
            }
        }

        #[tokio::test]
        async fn hardcoded_result_should_work() {
            test_utils::set_up();
            let fibo_webhook_harness = SetUpFiboWebhook::new().await;
            assert_eq!(
                "fiboa(1, 0) = hardcoded: 1",
                fibo_webhook_harness.fetch(1, 0, 200).await
            );
        }

        #[tokio::test]
        async fn direct_call_should_work() {
            test_utils::set_up();
            let fibo_webhook_harness = SetUpFiboWebhook::new().await;
            assert_eq!(
                "fiboa(2, 1) = direct call: 1",
                fibo_webhook_harness.fetch(2, 1, 200).await
            );
        }

        #[tokio::test]
        async fn scheduling_should_work() {
            test_utils::set_up();
            let fibo_webhook_harness = SetUpFiboWebhook::new().await;
            let resp = fibo_webhook_harness.fetch(10, 1, 200).await;
            let execution_id = resp.strip_prefix("fiboa(10, 1) = scheduled: ").unwrap();
            let execution_id = ExecutionId::from_str(execution_id).unwrap();
            let conn = fibo_webhook_harness.db_pool.connection();
            let res = conn
                .wait_for_finished_result(&execution_id, None)
                .await
                .unwrap();
            let res = assert_matches!(res, SupportedFunctionReturnValue::Ok{ok: Some(val)} => val);
            let (fibo, ok_ty) =
                assert_matches!(res, WastValWithType {value, r#type: ok_ty } => (value, ok_ty));
            assert_matches!(ok_ty, TypeWrapper::U64);
            let res = assert_matches!(fibo, WastVal::U64(val) => val);
            assert_eq!(FIBO_10_OUTPUT, res);
            fibo_webhook_harness.close().await;
        }

        #[tokio::test]
        async fn test_routing_error_handling() {
            test_utils::set_up();
            let fibo_webhook_harness = SetUpFiboWebhook::new().await;
            // Check wrong URL
            let resp = reqwest::get(format!(
                "http://{}/unknown",
                &fibo_webhook_harness.server_addr
            ))
            .await
            .unwrap();
            assert_eq!(resp.status().as_u16(), 404);
            assert_eq!("Route not found", resp.text().await.unwrap());
            // Check panicking inside WASM before response is streamed
            let resp = reqwest::get(format!(
                "http://{}/fibo/0/1",
                &fibo_webhook_harness.server_addr
            ))
            .await
            .unwrap();
            assert_eq!(resp.status().as_u16(), 500);
            assert_eq!("Component Error", resp.text().await.unwrap());
            fibo_webhook_harness.close().await;
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
                .find(&Method::GET, &Uri::from_static("/foo/"))
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
