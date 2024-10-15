use crate::component_logger::{log_activities, ComponentLogger};
use crate::envvar::EnvVar;
use crate::host_exports::{
    execution_id_into_val, execution_id_into_wast_val, val_to_join_set_id, DurationEnum,
    ValToJoinSetIdError, SUFFIX_FN_AWAIT_NEXT, SUFFIX_FN_SCHEDULE, SUFFIX_FN_SUBMIT,
};
use crate::std_output_stream::{LogStream, StdOutput};
use crate::{host_exports, WasmFileError, NAMESPACE_OBELISK_WITH_COLON};
use concepts::prefixed_ulid::JoinSetId;
use concepts::storage::{
    ClientError, CreateRequest, DbConnection, DbError, DbPool, ExecutionEventInner, HistoryEvent,
    HistoryEventScheduledAt, JoinSetRequest, JoinSetResponse, JoinSetResponseEvent, Version,
};
use concepts::{
    ConfigId, ExecutionId, ExecutionMetadata, FinishedExecutionError, FunctionFqn,
    FunctionMetadata, FunctionRegistry, IfcFqnName, ImportableType, Params, StrVariant,
    SupportedFunctionReturnValue,
};
use derivative::Derivative;
use http_body_util::combinators::BoxBody;
use hyper::body::Bytes;
use hyper::server::conn::http1;
use hyper::{Method, StatusCode, Uri};
use hyper_util::rt::TokioIo;
use route_recognizer::{Match, Router};
use std::marker::PhantomData;
use std::ops::Deref;
use std::path::Path;
use std::time::Duration;
use std::{fmt::Debug, sync::Arc};
use tokio::net::TcpListener;
use tracing::{debug, error, info, instrument, trace, warn, Instrument, Level, Span};
use utils::time::ClockFn;
use utils::wasm_tools::{ExIm, WasmComponent, SUFFIX_PKG_EXT};
use val_json::wast_val::WastVal;
use wasmtime::component::ResourceTable;
use wasmtime::component::{Linker, Val};
use wasmtime::{Engine, Store, UpdateDeadline};
use wasmtime_wasi::{WasiCtx, WasiCtxBuilder, WasiView};
use wasmtime_wasi_http::bindings::http::types::Scheme;
use wasmtime_wasi_http::bindings::ProxyPre;
use wasmtime_wasi_http::body::HyperOutgoingBody;
use wasmtime_wasi_http::{WasiHttpCtx, WasiHttpView};

const WASI_NAMESPACE_WITH_COLON: &str = "wasi:";

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct HttpTriggerConfig {
    pub config_id: ConfigId,
}
type StdError = Box<dyn std::error::Error + Send + Sync>;

#[derive(Debug, thiserror::Error)]
pub enum WebhookServerError {
    #[error("socket error: {0}")]
    SocketError(std::io::Error),
}

pub struct WebhookCompiled {
    pub config_id: ConfigId,
    forward_stdout: Option<StdOutput>,
    forward_stderr: Option<StdOutput>,
    env_vars: Arc<[EnvVar]>,
    retry_config: RetryConfigOverride,

    wasm_component: WasmComponent,
}

impl WebhookCompiled {
    pub fn new(
        wasm_path: impl AsRef<Path>,
        engine: &Engine,
        config_id: ConfigId,
        forward_stdout: Option<StdOutput>,
        forward_stderr: Option<StdOutput>,
        env_vars: Arc<[EnvVar]>,
        retry_config: RetryConfigOverride,
    ) -> Result<Self, WasmFileError> {
        let wasm_component = WasmComponent::new(wasm_path, engine)?;
        Ok(Self {
            config_id,
            forward_stdout,
            forward_stderr,
            env_vars,
            retry_config,
            wasm_component,
        })
    }

    #[must_use]
    pub fn imports(&self) -> &[FunctionMetadata] {
        &self.wasm_component.exim.imports_flat
    }

    #[instrument(skip_all, fields(config_id = %self.config_id), err)]
    pub fn link<C: ClockFn, DB: DbConnection, P: DbPool<DB>>(
        self,
        engine: &Engine,
        fn_registry: &dyn FunctionRegistry,
    ) -> Result<WebhookInstance<C, DB, P>, WasmFileError> {
        let mut linker = Linker::new(engine);
        // Link wasi
        wasmtime_wasi::add_to_linker_async(&mut linker).map_err(|err| {
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
        // Link obelisk: host-activities and log
        WebhookCtx::add_to_linker(&mut linker)?;

        // Mock imported functions
        for import in fn_registry.all_exports() {
            if import
                .ifc_fqn
                .deref()
                .starts_with(NAMESPACE_OBELISK_WITH_COLON)
                || import
                    .ifc_fqn
                    .deref()
                    .starts_with(WASI_NAMESPACE_WITH_COLON)
            {
                warn!(ifc_fqn = %import.ifc_fqn, "Skipping mock of reserved interface");
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
                            // FIXME: Add test for error message stability
                            debug!("Skipping mocking of {ffqn}");
                        } else {
                            return Err(WasmFileError::LinkingError {
                                context: StrVariant::Arc(Arc::from(format!(
                                    "cannot add mock for imported function {ffqn}"
                                ))),
                                err: err.into(),
                            });
                        }
                    }
                }
            } else {
                trace!("Skipping interface {ifc_fqn}", ifc_fqn = import.ifc_fqn);
            }
        }

        // Pre-instantiate to catch missing imports
        let proxy_pre = linker
            .instantiate_pre(&self.wasm_component.wasmtime_component)
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

        Ok(WebhookInstance {
            config_id: self.config_id,
            forward_stdout: self.forward_stdout,
            forward_stderr: self.forward_stderr,
            env_vars: self.env_vars,
            retry_config: self.retry_config,
            exim: self.wasm_component.exim,
            proxy_pre,
        })
    }
}

#[derive(Derivative)]
#[derivative(Clone(bound = ""))] // Clone only because of potentially registering 2 paths via `route-recognizer`
#[derivative(Debug)]
pub struct WebhookInstance<C: ClockFn, DB: DbConnection, P: DbPool<DB>> {
    #[derivative(Debug = "ignore")]
    proxy_pre: Arc<ProxyPre<WebhookCtx<C, DB, P>>>,
    pub config_id: ConfigId,
    forward_stdout: Option<StdOutput>,
    forward_stderr: Option<StdOutput>,
    env_vars: Arc<[EnvVar]>,
    retry_config: RetryConfigOverride,
    exim: ExIm,
}

impl<C: ClockFn, DB: DbConnection, P: DbPool<DB>> WebhookInstance<C, DB, P> {
    #[must_use]
    pub fn imported_functions(&self) -> &[FunctionMetadata] {
        &self.exim.imports_flat
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

#[expect(clippy::too_many_arguments)]
pub async fn server<C: ClockFn + 'static, DB: DbConnection + 'static, P: DbPool<DB> + 'static>(
    listener: TcpListener,
    engine: Arc<Engine>,
    router: MethodAwareRouter<WebhookInstance<C, DB, P>>,
    db_pool: P,
    clock_fn: C,
    fn_registry: Arc<dyn FunctionRegistry>,
    request_timeout: Duration,
    task_limiter: Option<Arc<tokio::sync::Semaphore>>,
) -> Result<(), WebhookServerError> {
    let router = Arc::new(router);
    loop {
        let (stream, _) = listener
            .accept()
            .await
            .map_err(WebhookServerError::SocketError)?;
        let io = TokioIo::new(stream);
        let task_limiter_guard = if let Some(task_limiter) = task_limiter.clone() {
            task_limiter.try_acquire_owned().map(Some)
        } else {
            Ok(None)
        };
        if let Ok(task_limiter_guard) = task_limiter_guard {
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
                                let execution_id = ExecutionId::generate();
                                debug!(%execution_id, method = %req.method(), uri = %req.uri(), "Processing request");
                                RequestHandler {
                                    engine: engine.clone(),
                                    clock_fn: clock_fn.clone(),
                                    db_pool: db_pool.clone(),
                                    fn_registry: fn_registry.clone(),
                                    request_timeout,
                                    execution_id,
                                    router: router.clone(),
                                    phantom_data: PhantomData,
                                }
                                .handle_request(req)
                            }),
                        )
                        .await;
                    if let Err(err) = res {
                        error!("Error serving connection: {err:?}");
                    }
                    drop(task_limiter_guard);
                }
            });
        } else {
            let _ = http1::Builder::new()
                .serve_connection(
                    io,
                    hyper::service::service_fn(move |req| {
                        debug!(method = %req.method(), uri = %req.uri(), "Out of permits");
                        std::future::ready(Ok::<_, hyper::Error>(resp(
                            "Out of permits",
                            StatusCode::SERVICE_UNAVAILABLE,
                        )))
                    }),
                )
                .await;
        }
    }
}

#[derive(Clone, Copy, Default, Debug)]
pub struct RetryConfigOverride {
    activity_max_retries_override: Option<u32>,
    activity_retry_exp_backoff_override: Option<Duration>,
}

impl RetryConfigOverride {
    fn max_retries(&self, import_type: ImportableType, component_default: u32) -> u32 {
        match import_type {
            ImportableType::ActivityWasm => self
                .activity_max_retries_override
                .unwrap_or(component_default),
            ImportableType::Workflow => 0,
        }
    }

    fn retry_exp_backoff(
        &self,
        import_type: ImportableType,
        component_default: Duration,
    ) -> Duration {
        match import_type {
            ImportableType::ActivityWasm => self
                .activity_retry_exp_backoff_override
                .unwrap_or(component_default),
            ImportableType::Workflow => Duration::ZERO,
        }
    }
}

struct WebhookCtx<C: ClockFn, DB: DbConnection, P: DbPool<DB>> {
    config_id: ConfigId,
    clock_fn: C,
    db_pool: P,
    fn_registry: Arc<dyn FunctionRegistry>,
    table: ResourceTable,
    wasi_ctx: WasiCtx,
    http_ctx: WasiHttpCtx,
    retry_config: RetryConfigOverride,
    responses: Vec<(JoinSetResponseEvent, ProcessingStatus)>,
    execution_id: ExecutionId,
    version: Option<Version>,
    component_logger: ComponentLogger,
    phantom_data: PhantomData<DB>,
}

#[derive(PartialEq, Eq, Clone, Copy, Debug)]
enum ProcessingStatus {
    Unprocessed,
    Processed,
}

#[derive(thiserror::Error, Debug, Clone)]
enum WebhookFunctionError {
    #[error("sumbitting failed, metadata for {ffqn} not found")]
    FunctionMetadataNotFound { ffqn: FunctionFqn },
    #[error(transparent)]
    DbError(#[from] DbError),
    #[error(transparent)]
    FinishedExecutionError(#[from] FinishedExecutionError),
    #[error("uncategorized error: {0}")]
    UncategorizedError(&'static str),
}

const HTTP_HANDLER_FFQN: FunctionFqn =
    FunctionFqn::new_static("wasi:http/incoming-handler", "handle");

impl<C: ClockFn, DB: DbConnection, P: DbPool<DB>> WebhookCtx<C, DB, P> {
    // Create new execution if this is the first call of the request/response cycle
    async fn get_version(&mut self) -> Result<Version, DbError> {
        if let Some(found) = &self.version {
            return Ok(found.clone());
        }
        let created_at = self.clock_fn.now();
        // Associate the (root) request execution with the request span. Makes possible to find the trace by execution id.
        let metadata = concepts::ExecutionMetadata::from_parent_span(&self.component_logger.span);
        let create_request = CreateRequest {
            created_at,
            execution_id: self.execution_id,
            ffqn: HTTP_HANDLER_FFQN,
            params: Params::empty(),
            parent: None,
            metadata,
            scheduled_at: created_at,
            max_retries: 0,
            retry_exp_backoff: Duration::ZERO,
            config_id: self.config_id.clone(),
            return_type: None,
            topmost_parent: self.execution_id,
        };
        let conn = self.db_pool.connection();
        let version = conn.create(create_request).await?;
        self.version = Some(version.clone());
        Ok(version)
    }

    // No support for combined delay finished / child finished yet.
    fn find_response_child_finished(
        &mut self,
        join_set_id: JoinSetId,
        results: &mut [Val],
    ) -> Result<Option<()>, WebhookFunctionError> {
        if results.len() != 1 {
            // either
            // result<execution-id, tuple<execution-id, error-result>> or
            // result<tuple<execution-id>, inner>, tuple<execution-id, error-result>>
            error!("Unexpected results length for -await-next");
            return Err(WebhookFunctionError::UncategorizedError(
                "Unexpected results length for -await-next",
            ));
        }
        if let Some((response, status)) = self
            .responses
            .iter_mut()
            .find(|(r, s)| *s == ProcessingStatus::Unprocessed && r.join_set_id == join_set_id)
        {
            *status = ProcessingStatus::Processed;
            if let JoinSetResponseEvent {
                event:
                    JoinSetResponse::ChildExecutionFinished {
                        result,
                        child_execution_id,
                    },
                join_set_id: _,
            } = response
            {
                trace!("Found child response: {result:?}");
                // TODO: If the child execution succeeded, perform type check between `SupportedFunctionReturnValue`
                // and what is expected by the `FunctionRegistry`
                let child_execution_id = execution_id_into_wast_val(*child_execution_id);
                match result {
                    Ok(
                        SupportedFunctionReturnValue::Fallible(v)
                        | SupportedFunctionReturnValue::Infallible(v),
                    ) => {
                        // Ok(tuple<execution-id, (original return value)>)
                        results[0] = WastVal::Result(Ok(Some(Box::new(WastVal::Tuple(vec![
                            child_execution_id,
                            v.value.clone(),
                        ])))))
                        .as_val();
                    }
                    Ok(SupportedFunctionReturnValue::None) => {
                        results[0] =
                            WastVal::Result(Ok(Some(Box::new(child_execution_id)))).as_val();
                    }

                    Err(err) => {
                        let variant = match err {
                            FinishedExecutionError::PermanentTimeout => {
                                WastVal::Variant("permanent-timeout".to_string(), None)
                            }
                            FinishedExecutionError::NondeterminismDetected(_) => {
                                WastVal::Variant("non-determinism".to_string(), None)
                            }
                            FinishedExecutionError::PermanentFailure(reason) => WastVal::Variant(
                                "permanent-failure".to_string(),
                                Some(Box::new(WastVal::String(reason.to_string()))),
                            ),
                        };
                        // Err(tuple<execution-id, execution-error>)
                        results[0] = WastVal::Result(Err(Some(Box::new(WastVal::Tuple(vec![
                            child_execution_id,
                            variant,
                        ])))))
                        .as_val();
                    }
                }
                Ok(Some(()))
            } else {
                Err(WebhookFunctionError::UncategorizedError(
                    "wrong join set id",
                ))
            }
        } else {
            Ok(None)
        }
    }

    #[instrument(level = Level::DEBUG, skip_all, fields(%ffqn, version), err)]
    async fn call_imported_fn(
        &mut self,
        ffqn: FunctionFqn,
        params: &[Val],
        results: &mut [Val],
    ) -> Result<(), WebhookFunctionError> {
        debug!(?params, "call_imported_fn start");
        if let Some(package_name) = ffqn.ifc_fqn.package_name().strip_suffix(SUFFIX_PKG_EXT) {
            let ifc_fqn = IfcFqnName::from_parts(
                ffqn.ifc_fqn.namespace(),
                package_name,
                ffqn.ifc_fqn.ifc_name(),
                ffqn.ifc_fqn.version(),
            );
            if let Some(function_name) = ffqn.function_name.strip_suffix(SUFFIX_FN_SUBMIT) {
                let ffqn =
                    FunctionFqn::new_arc(Arc::from(ifc_fqn.to_string()), Arc::from(function_name));
                debug!("Got `-submit` extension for {ffqn}");
                let Some((join_set_id, params)) = params.split_first() else {
                    error!("Got empty params, expected JoinSetId");
                    return Err(WebhookFunctionError::UncategorizedError(
                        "error running `-submit` extension function: exepcted at least one parameter with JoinSetId, got empty parameter list",
                    ));
                };
                let join_set_id = val_to_join_set_id(join_set_id)
                    .map_err(|err|WebhookFunctionError::UncategorizedError(match err {
                        ValToJoinSetIdError::ParseError => "error running `-submit` extension function: cannot parse join-set-id",
                        ValToJoinSetIdError::TypeError => "error running `-submit` extension function: wrong first parameter type, expected join-set-id",
                    }))?;
                let version = self.get_version().await?;
                let span = Span::current();
                span.record("version", tracing::field::display(&version));
                let child_execution_id = ExecutionId::generate();
                let Some((function_metadata, config_id, child_retry_config, import_type)) =
                    self.fn_registry.get_by_exported_function(&ffqn).await
                else {
                    return Err(WebhookFunctionError::FunctionMetadataNotFound { ffqn });
                };
                // Write to db
                let created_at = self.clock_fn.now();
                let child_exec_req = ExecutionEventInner::HistoryEvent {
                    event: HistoryEvent::JoinSetRequest {
                        join_set_id,
                        request: JoinSetRequest::ChildExecutionRequest { child_execution_id },
                    },
                };
                let create_child_req = CreateRequest {
                    created_at,
                    execution_id: child_execution_id,
                    ffqn,
                    params: Params::from_wasmtime(Arc::from(params)),
                    parent: Some((self.execution_id, join_set_id)),
                    metadata: ExecutionMetadata::from_parent_span(&self.component_logger.span),
                    scheduled_at: created_at,
                    max_retries: self
                        .retry_config
                        .max_retries(import_type, child_retry_config.max_retries),
                    retry_exp_backoff: self
                        .retry_config
                        .retry_exp_backoff(import_type, child_retry_config.retry_exp_backoff),
                    config_id,
                    return_type: function_metadata.return_type.map(|rt| rt.type_wrapper),
                    topmost_parent: self.execution_id,
                };
                let db_connection = self.db_pool.connection();
                let version = db_connection
                    .append_batch_create_new_execution(
                        created_at,
                        vec![child_exec_req],
                        self.execution_id,
                        version.clone(),
                        vec![create_child_req],
                    )
                    .await?;
                self.version = Some(version);
                results[0] = execution_id_into_val(child_execution_id);
                Ok(())
            } else if let Some(function_name) =
                ffqn.function_name.strip_suffix(SUFFIX_FN_AWAIT_NEXT)
            {
                let version = self.get_version().await?;
                tracing::Span::current().record("version", tracing::field::display(version));
                debug!("Got await-next extension for function `{function_name}`");
                let [join_set_id] = params else {
                    error!("Expected single parameter with join-set-id got {params:?}");
                    return Err(WebhookFunctionError::UncategorizedError(
                        "error running `-await-next` extension function: wrong parameter length, expected single string parameter containing join-set-id"
                    ));
                };
                let join_set_id = val_to_join_set_id(join_set_id).map_err(|err| WebhookFunctionError::UncategorizedError(match err {
                    ValToJoinSetIdError::ParseError => "error running `-await-next` extension function: cannot parse join-set-id",
                    ValToJoinSetIdError::TypeError => "error running `-await-next` extension function: wrong parameter type, expected single string parameter containing join-set-id",
                }))?;

                let conn = self.db_pool.connection();
                while self
                    .find_response_child_finished(join_set_id, results)?
                    .is_none()
                {
                    let next_responses = conn
                        .subscribe_to_next_responses(self.execution_id, self.responses.len())
                        .await?;
                    debug!("Got next responses {next_responses:?}");
                    self.responses.extend(
                        next_responses
                            .into_iter()
                            .map(|outer| (outer.event, ProcessingStatus::Unprocessed)),
                    );
                    trace!("All responses: {:?}", self.responses);
                }
                Ok(())
            } else if let Some(function_name) = ffqn.function_name.strip_suffix(SUFFIX_FN_SCHEDULE)
            {
                let ffqn =
                    FunctionFqn::new_arc(Arc::from(ifc_fqn.to_string()), Arc::from(function_name));
                debug!("Got `-schedule` extension for {ffqn}");
                let Some((scheduled_at, params)) = params.split_first() else {
                    error!("Error running `-schedule` extension function: exepcted at least one parameter of type `scheduled-at`, got empty parameter list");
                    return Err(WebhookFunctionError::UncategorizedError(
                        "error running `-schedule` extension function: exepcted at least one parameter of type `scheduled-at`, got empty parameter list",
                    ));
                };
                let scheduled_at = match HistoryEventScheduledAt::try_from(scheduled_at) {
                    Ok(scheduled_at) => scheduled_at,
                    Err(err) => {
                        error!("Wrong type for the first `-scheduled-at` parameter, expected `scheduled-at`, got `{scheduled_at:?}` - {err:?}");
                        return Err(WebhookFunctionError::UncategorizedError(
                            "error running `-schedule` extension function: wrong first parameter type"
                        ));
                    }
                };
                // Write to db
                let version = self.get_version().await?;
                let span = Span::current();
                span.record("version", tracing::field::display(&version));
                let new_execution_id = ExecutionId::generate();
                let Some((function_metadata, config_id, default_retry_config, import_type)) =
                    self.fn_registry.get_by_exported_function(&ffqn).await
                else {
                    return Err(WebhookFunctionError::FunctionMetadataNotFound { ffqn });
                };
                let created_at = self.clock_fn.now();

                let event = HistoryEvent::Schedule {
                    execution_id: new_execution_id,
                    scheduled_at,
                };
                let scheduled_at = scheduled_at.as_date_time(created_at);
                let child_exec_req = ExecutionEventInner::HistoryEvent { event };

                let create_child_req = CreateRequest {
                    created_at,
                    execution_id: new_execution_id,
                    ffqn,
                    params: Params::from_wasmtime(Arc::from(params)),
                    parent: None, // Schedule breaks from the parent-child relationship to avoid a linked list
                    metadata: ExecutionMetadata::from_linked_span(&self.component_logger.span),
                    scheduled_at,
                    max_retries: self
                        .retry_config
                        .max_retries(import_type, default_retry_config.max_retries),
                    retry_exp_backoff: self
                        .retry_config
                        .retry_exp_backoff(import_type, default_retry_config.retry_exp_backoff),
                    config_id,
                    return_type: function_metadata.return_type.map(|rt| rt.type_wrapper),
                    topmost_parent: self.execution_id,
                };
                let db_connection = self.db_pool.connection();
                let version = db_connection
                    .append_batch_create_new_execution(
                        created_at,
                        vec![child_exec_req],
                        self.execution_id,
                        version.clone(),
                        vec![create_child_req],
                    )
                    .await?;
                self.version = Some(version);
                results[0] = execution_id_into_val(new_execution_id);
                Ok(())
            } else {
                error!("unrecognized extension function {ffqn}");
                return Err(WebhookFunctionError::UncategorizedError(
                    "unrecognized extension function",
                ));
            }
        } else {
            // direct call
            let version = self.get_version().await?;
            let span = Span::current();
            span.record("version", tracing::field::display(&version));
            let child_execution_id = ExecutionId::generate();
            let created_at = self.clock_fn.now();
            let Some((function_metadata, config_id, default_retry_config, import_type)) =
                self.fn_registry.get_by_exported_function(&ffqn).await
            else {
                return Err(WebhookFunctionError::FunctionMetadataNotFound { ffqn });
            };
            let join_set_id = JoinSetId::generate();
            let child_exec_req = ExecutionEventInner::HistoryEvent {
                event: HistoryEvent::JoinSetRequest {
                    join_set_id,
                    request: JoinSetRequest::ChildExecutionRequest { child_execution_id },
                },
            };
            let create_child_req = CreateRequest {
                created_at,
                execution_id: child_execution_id,
                ffqn,
                params: Params::from_wasmtime(Arc::from(params)),
                parent: Some((self.execution_id, join_set_id)),
                metadata: ExecutionMetadata::from_parent_span(&self.component_logger.span),
                scheduled_at: created_at,
                max_retries: self
                    .retry_config
                    .max_retries(import_type, default_retry_config.max_retries),
                retry_exp_backoff: self
                    .retry_config
                    .retry_exp_backoff(import_type, default_retry_config.retry_exp_backoff),
                config_id,
                return_type: function_metadata.return_type.map(|rt| rt.type_wrapper),
                topmost_parent: self.execution_id,
            };
            let db_connection = self.db_pool.connection();
            let version = db_connection
                .append_batch_create_new_execution(
                    created_at,
                    vec![child_exec_req],
                    self.execution_id,
                    version.clone(),
                    vec![create_child_req],
                )
                .await?;
            self.version = Some(version);

            let res = match db_connection
                .wait_for_finished_result(child_execution_id, None /* TODO timeouts */)
                .await
            {
                Ok(res) => res?,
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

    fn add_to_linker(linker: &mut Linker<WebhookCtx<C, DB, P>>) -> Result<(), WasmFileError> {
        host_exports::obelisk::workflow::host_activities::add_to_linker(
            linker,
            |state: &mut Self| state,
        )
        .map_err(|err| WasmFileError::LinkingError {
            context: StrVariant::Static("linking host activities"),
            err: err.into(),
        })?;
        log_activities::obelisk::log::log::add_to_linker(linker, |state: &mut Self| state)
            .map_err(|err| WasmFileError::LinkingError {
                context: StrVariant::Static("linking log activities"),
                err: err.into(),
            })?;
        Ok(())
    }
}

impl<C: ClockFn, DB: DbConnection, P: DbPool<DB>> WebhookCtx<C, DB, P> {
    #[must_use]
    #[expect(clippy::too_many_arguments)]
    fn new<'a>(
        config_id: ConfigId,
        engine: &Engine,
        clock_fn: C,
        db_pool: P,
        fn_registry: Arc<dyn FunctionRegistry>,
        retry_config: RetryConfigOverride,
        params: impl Iterator<Item = (&'a str, &'a str)>,
        execution_id: ExecutionId,
        forward_stdout: Option<StdOutput>,
        forward_stderr: Option<StdOutput>,
        env_vars: &[EnvVar],
        request_span: Span,
    ) -> Store<WebhookCtx<C, DB, P>> {
        let mut wasi_ctx = WasiCtxBuilder::new();
        if let Some(stdout) = forward_stdout {
            let stdout = LogStream::new(format!("[{config_id} {execution_id} stdout]"), stdout);
            wasi_ctx.stdout(stdout);
        }
        if let Some(stderr) = forward_stderr {
            let stderr = LogStream::new(format!("[{config_id} {execution_id} stderr]"), stderr);
            wasi_ctx.stderr(stderr);
        }
        for env_var in env_vars {
            wasi_ctx.env(&env_var.key, &env_var.val);
        }
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
            version: None,
            responses: Vec::new(),
            config_id,
            execution_id,
            component_logger: ComponentLogger { span: request_span },
            phantom_data: PhantomData,
        };
        let mut store = Store::new(engine, ctx);
        store.epoch_deadline_callback(|_store_ctx| Ok(UpdateDeadline::Yield(1)));
        store
    }
}

#[async_trait::async_trait]
impl<C: ClockFn, DB: DbConnection, P: DbPool<DB>>
    host_exports::obelisk::workflow::host_activities::Host for WebhookCtx<C, DB, P>
{
    async fn sleep(&mut self, duration: DurationEnum) -> wasmtime::Result<()> {
        tokio::time::sleep(Duration::from(duration)).await;
        Ok(())
    }

    async fn new_join_set(
        &mut self,
    ) -> wasmtime::Result<crate::host_exports::obelisk::types::execution::JoinSetId> {
        let join_set_id = JoinSetId::generate();
        Ok(host_exports::obelisk::types::execution::JoinSetId {
            id: join_set_id.to_string(),
        })
    }
}

impl<C: ClockFn, DB: DbConnection, P: DbPool<DB>> log_activities::obelisk::log::log::Host
    for WebhookCtx<C, DB, P>
{
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

struct RequestHandler<C: ClockFn + 'static, DB: DbConnection + 'static, P: DbPool<DB> + 'static> {
    engine: Arc<Engine>,
    clock_fn: C,
    db_pool: P,
    fn_registry: Arc<dyn FunctionRegistry>,
    request_timeout: Duration,
    execution_id: ExecutionId,
    router: Arc<MethodAwareRouter<WebhookInstance<C, DB, P>>>,
    phantom_data: PhantomData<DB>,
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

impl<C: ClockFn + 'static, DB: DbConnection + 'static, P: DbPool<DB> + 'static>
    RequestHandler<C, DB, P>
{
    #[instrument(skip_all, name="incoming webhook request", fields(execution_id = %self.execution_id))]
    async fn handle_request(
        self,
        req: hyper::Request<hyper::body::Incoming>,
    ) -> Result<hyper::Response<HyperOutgoingBody>, hyper::Error> {
        self.handle_request_inner(req, Span::current())
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
                })
            })
    }

    async fn handle_request_inner(
        self,
        req: hyper::Request<hyper::body::Incoming>,
        request_span: Span,
    ) -> Result<hyper::Response<HyperOutgoingBody>, HandleRequestError> {
        #[derive(Debug, thiserror::Error)]
        #[error("timeout")]
        struct TimeoutError;

        if let Some(matched) = self.router.find(req.method(), req.uri()) {
            let found_instance = matched.handler();
            let (sender, receiver) = tokio::sync::oneshot::channel();
            let mut store = WebhookCtx::new(
                found_instance.config_id.clone(),
                &self.engine,
                self.clock_fn,
                self.db_pool,
                self.fn_registry,
                found_instance.retry_config,
                matched.params().iter(),
                self.execution_id,
                found_instance.forward_stdout,
                found_instance.forward_stderr,
                &found_instance.env_vars,
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

            let task = tokio::task::spawn(
                async move {
                    tokio::select! {
                        result = proxy.wasi_http_incoming_handler().call_handle(store, req, out)=> {
                            result.inspect_err(|err| error!("Webhook instance returned error: {err:?}"))
                        },
                        () = tokio::time::sleep(self.request_timeout) => {
                            info!("Timing out the request");
                            Err(TimeoutError.into())
                        }
                    }
                }
                .instrument(request_span),
            );
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
    #[error("timeout")]
    Timeout,
}

#[cfg(test)]
pub(crate) mod tests {
    use super::MethodAwareRouter;
    use hyper::{Method, Uri};

    #[cfg(not(madsim))] // Due to TCP server/client
    pub(crate) mod nosim {
        use super::*;
        use crate::activity::activity_worker::tests::{compile_activity, FIBO_10_OUTPUT};
        use crate::engines::{EngineConfig, Engines};
        use crate::tests::TestingFnRegistry;
        use crate::webhook::webhook_trigger::{self, RetryConfigOverride, WebhookCompiled};
        use crate::workflow::workflow_worker::tests::compile_workflow;
        use crate::{
            activity::activity_worker::tests::spawn_activity_fibo,
            workflow::workflow_worker::{tests::spawn_workflow_fibo, JoinNextBlockingStrategy},
        };
        use assert_matches::assert_matches;
        use concepts::{
            storage::{DbConnection, DbPool},
            ExecutionId,
        };
        use concepts::{ConfigId, SupportedFunctionReturnValue};
        use db_tests::{Database, DbGuard, DbPoolEnum};
        use executor::executor::ExecutorTaskHandle;
        use std::net::SocketAddr;
        use std::str::FromStr;
        use std::sync::Arc;
        use std::time::Duration;
        use test_utils::sim_clock::SimClock;
        use tokio::net::TcpListener;
        use tracing::info;
        use utils::wasm_tools::WasmComponent;
        use val_json::type_wrapper::TypeWrapper;
        use val_json::wast_val::{WastVal, WastValWithType};

        struct AbortOnDrop(tokio::task::AbortHandle);
        impl Drop for AbortOnDrop {
            fn drop(&mut self) {
                self.0.abort();
            }
        }

        pub(crate) async fn compile_webhook(wasm_path: &str) -> WasmComponent {
            let engine =
                Engines::get_webhook_engine(EngineConfig::on_demand_testing().await).unwrap();
            WasmComponent::new(wasm_path, &engine).unwrap()
        }

        struct SetUpFiboWebhook {
            _server: AbortOnDrop,
            #[expect(dead_code)]
            guard: DbGuard,
            db_pool: DbPoolEnum,
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
                    spawn_activity_fibo(db_pool.clone(), sim_clock.clone()).await;
                let fn_registry = TestingFnRegistry::new_from_components(vec![
                    compile_activity(
                        test_programs_fibo_activity_builder::TEST_PROGRAMS_FIBO_ACTIVITY,
                    )
                    .await,
                    compile_workflow(
                        test_programs_fibo_workflow_builder::TEST_PROGRAMS_FIBO_WORKFLOW,
                    )
                    .await,
                ]);
                let engine =
                    Engines::get_webhook_engine(EngineConfig::on_demand_testing().await).unwrap();
                let workflow_exec_task = spawn_workflow_fibo(
                    db_pool.clone(),
                    sim_clock.clone(),
                    JoinNextBlockingStrategy::Await,
                    0,
                    fn_registry.clone(),
                )
                .await;

                let router = {
                    let instance = WebhookCompiled::new(
                        test_programs_fibo_webhook_builder::TEST_PROGRAMS_FIBO_WEBHOOK,
                        &engine,
                        ConfigId::dummy_activity(),
                        None,
                        None,
                        Arc::from([]),
                        RetryConfigOverride::default(),
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
                        tcp_listener,
                        engine,
                        router,
                        db_pool.clone(),
                        sim_clock.clone(),
                        fn_registry,
                        Duration::from_secs(1),
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

            async fn fetch(&self, n: u8, expected_status_code: u16) -> String {
                let resp = reqwest::get(format!("http://{}/fibo/{n}/1", &self.server_addr))
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
                "fiboa(1, 1) = hardcoded: 1",
                fibo_webhook_harness.fetch(1, 200).await
            );
        }

        #[tokio::test]
        async fn direct_call_should_work() {
            test_utils::set_up();
            let fibo_webhook_harness = SetUpFiboWebhook::new().await;
            assert_eq!(
                "fiboa(2, 1) = direct call: 1",
                fibo_webhook_harness.fetch(2, 200).await
            );
        }

        #[tokio::test]
        async fn submitting_and_not_waiting_should_work() {
            test_utils::set_up();
            let fibo_webhook_harness = SetUpFiboWebhook::new().await;
            let resp = fibo_webhook_harness.fetch(10, 200).await;
            let execution_id = resp.strip_prefix("fiboa(10, 1) = submitted: ").unwrap();
            let execution_id = ExecutionId::from_str(execution_id).unwrap();
            let conn = fibo_webhook_harness.db_pool.connection();
            let res = conn
                .wait_for_finished_result(execution_id, None)
                .await
                .unwrap()
                .unwrap();
            let res = assert_matches!(res, SupportedFunctionReturnValue::Infallible(val) => val);
            let res = assert_matches!(res, WastValWithType{ value: WastVal::U64(actual), r#type: TypeWrapper::U64} => actual);
            assert_eq!(FIBO_10_OUTPUT, res,);
            fibo_webhook_harness.close().await;
        }

        #[tokio::test]
        async fn submit_await_next_should_work() {
            test_utils::set_up();
            let fibo_webhook_harness = SetUpFiboWebhook::new().await;
            assert_eq!(
                "fiboa(5, 1) = submit/await-next: 5",
                fibo_webhook_harness.fetch(5, 200).await
            );
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
                "http://{}/fibo/1a/1",
                &fibo_webhook_harness.server_addr
            ))
            .await
            .unwrap();
            assert_eq!(resp.status().as_u16(), 500);
            assert_eq!("Component Error", resp.text().await.unwrap());
            fibo_webhook_harness.close().await;
        }
    }

    // TODO: add timeout test

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
