use crate::component_logger::{ComponentLogger, LogStrageConfig, log_activities};
use crate::envvar::EnvVar;
use crate::http_hooks::{HttpClientTracesContainer, HttpHooks};
use crate::std_output_stream::{LogStream, StdOutput, StdOutputConfig, StdOutputConfigWithSender};
use crate::webhook::webhook_registry::WebhookStateWatcher;
use crate::webhook::webhook_trigger::types::obelisk::types::join_set::JoinNextError;
use crate::workflow::host_exports::{SUFFIX_FN_SCHEDULE, history_event_schedule_at_from_wast_val};
use crate::{RunnableComponent, WasmFileError};
use assert_matches::assert_matches;
use concepts::prefixed_ulid::{
    DeploymentId, ExecutionIdDerived, ExecutionIdTopLevel, JOIN_SET_START_IDX, RunId,
};
use concepts::storage::{
    AppendRequest, BacktraceInfo, CreateRequest, DbConnection, DbErrorGeneric, DbErrorRead,
    DbErrorReadWithTimeout, DbErrorWrite, DbPool, ExecutionRequest, HistoryEvent,
    HistoryEventScheduleAt, JoinSetRequest, LogInfoAppendRow, LogLevel, LogStreamType,
    PendingState, PendingStateFinishedError, PendingStateFinishedResultKind, TimeoutOutcome,
    Version, http_client_trace::HttpClientTrace,
};
use concepts::time::{ClockFn, Sleep};
use concepts::{
    ComponentId, ExecutionFailureKind, ExecutionId, ExecutionMetadata, FinishedExecutionError,
    FunctionFqn, FunctionMetadata, FunctionRegistry, IfcFqnName, JoinSetKind, Params, ReturnType,
    SUFFIX_PKG_SCHEDULE, SUPPORTED_RETURN_VALUE_OK_EMPTY, StrVariant, TrapKind,
};
use concepts::{JoinSetId, SupportedFunctionReturnValue};
use http_body_util::combinators::UnsyncBoxBody;
use hyper::body::Bytes;
use hyper::server::conn::http1;
use hyper::{Method, StatusCode, Uri};
use hyper_util::rt::TokioIo;
use log_activities::obelisk::log::log::Host;
use route_recognizer::{Match, Router};
use std::ops::Deref;
use std::pin::Pin;
use std::str::FromStr;
use std::time::Duration;
use std::{fmt::Debug, sync::Arc};
use tokio::net::TcpListener;
use tokio::select;
use tokio::sync::{OwnedSemaphorePermit, mpsc, watch};
use tracing::{
    Instrument, Span, debug, debug_span, error, info, info_span, instrument, trace, warn,
};
use types::obelisk::types::execution::Host as ExecutionHost;
use types::obelisk::types::join_set::HostJoinSet;
use types::obelisk::webhook::webhook_support::Host as WebhookSupportHost;
use val_json::wast_val::WastVal;
use wasmtime::component::ResourceTable;
use wasmtime::component::types::ComponentFunc;
use wasmtime::component::{Linker, Val};
use wasmtime::{Engine, Store, UpdateDeadline};
use wasmtime_wasi::{WasiCtx, WasiCtxBuilder, WasiCtxView, WasiView};
use wasmtime_wasi_http::WasiHttpCtx;
use wasmtime_wasi_http::p2::bindings::ProxyPre;
use wasmtime_wasi_http::p2::bindings::http::types::Scheme;
use wasmtime_wasi_http::p2::body::HyperOutgoingBody;
use wasmtime_wasi_http::p2::{WasiHttpCtxView, WasiHttpView};
use wasmtime_wasi_io::IoView;

const HTTP_HANDLER_FFQN: FunctionFqn =
    FunctionFqn::new_static("wasi:http/incoming-handler", "handle");

pub(crate) mod types {
    wasmtime::component::bindgen!({
        path: "host-wit-webhook/",
        inline: "package any:any;
                world bindings {
                    import obelisk:types/time@4.2.0;
                    import obelisk:types/execution@4.2.0;
                    import obelisk:types/backtrace@4.2.0;
                    import obelisk:types/join-set@4.2.0;
                    import obelisk:webhook/webhook-support@5.1.0;
                }",
        world: "any:any/bindings",
        imports: {
            // Make webhook-support functions async and trappable for infrastructure errors
            "obelisk:webhook/webhook-support": async | trappable,
        },
        with: {
            "obelisk:types/join-set.join-set": concepts::JoinSetId,
        },
        trappable_error_type: {
            "obelisk:types/execution.schedule-json-error" => crate::webhook::webhook_trigger::ScheduleJsonErrorTrappable,
            "obelisk:webhook/webhook-support.get-error" => crate::webhook::webhook_trigger::GetErrorTrappable,
            "obelisk:webhook/webhook-support.get-status-error" => crate::webhook::webhook_trigger::GetStatusErrorTrappable,
            "obelisk:webhook/webhook-support.try-get-error" => crate::webhook::webhook_trigger::TryGetErrorTrappable,
        },
    });
}

/// Trappable wrapper for `ScheduleJsonError` - user errors vs infrastructure failures.
#[derive(Debug, thiserror::Error)]
pub(crate) enum ScheduleJsonErrorTrappable {
    #[error(transparent)]
    Normal(#[from] types::obelisk::types::execution::ScheduleJsonError),
    #[error(transparent)]
    Trap(#[from] wasmtime::Error),
}

/// Trappable wrapper for `GetError` - user errors vs infrastructure failures.
#[derive(Debug, thiserror::Error)]
pub(crate) enum GetErrorTrappable {
    #[error(transparent)]
    Normal(#[from] types::obelisk::webhook::webhook_support::GetError),
    #[error(transparent)]
    Trap(#[from] wasmtime::Error),
}

/// Trappable wrapper for `GetStatusError` - user errors vs infrastructure failures.
#[derive(Debug, thiserror::Error)]
pub(crate) enum GetStatusErrorTrappable {
    #[error(transparent)]
    Normal(#[from] types::obelisk::webhook::webhook_support::GetStatusError),
    #[error(transparent)]
    Trap(#[from] wasmtime::Error),
}

/// Trappable wrapper for `TryGetError` - user errors vs infrastructure failures.
#[derive(Debug, thiserror::Error)]
pub(crate) enum TryGetErrorTrappable {
    #[error(transparent)]
    Normal(#[from] types::obelisk::webhook::webhook_support::TryGetError),
    #[error(transparent)]
    Trap(#[from] wasmtime::Error),
}

// Conversions from webhook types to internal types
/// Convert `SupportedFunctionReturnValue` to the JSON result format expected by webhook-support.
/// Returns Ok(Some(json)) for successful result with value,
/// Ok(None) for successful result with no value,
/// Err(Some(json)) for error result with value,
/// Err(None) for error result with no value.
fn supported_return_value_to_json_result(
    retval: &SupportedFunctionReturnValue,
) -> Result<Option<String>, Option<String>> {
    match retval {
        SupportedFunctionReturnValue::Ok(Some(val_with_type)) => {
            let json =
                serde_json::to_string(&val_with_type.value).unwrap_or_else(|_| "null".to_string());
            Ok(Some(json))
        }
        SupportedFunctionReturnValue::Ok(None) => Ok(None),
        SupportedFunctionReturnValue::Err(Some(val_with_type)) => {
            let json =
                serde_json::to_string(&val_with_type.value).unwrap_or_else(|_| "null".to_string());
            Err(Some(json))
        }
        SupportedFunctionReturnValue::Err(None) => Err(None),
        SupportedFunctionReturnValue::ExecutionError(err) => {
            Err(Some(format!("execution error: {err}")))
        }
    }
}

fn schedule_at_from_webhook(
    schedule_at: types::obelisk::webhook::webhook_support::ScheduleAt,
) -> HistoryEventScheduleAt {
    use chrono::{DateTime, Utc};
    use std::time::UNIX_EPOCH;
    use types::obelisk::webhook::webhook_support::ScheduleAt;

    match schedule_at {
        ScheduleAt::Now => HistoryEventScheduleAt::Now,
        ScheduleAt::At(datetime) => {
            let duration = Duration::new(datetime.seconds, datetime.nanoseconds);
            let systemtime = UNIX_EPOCH + duration;
            HistoryEventScheduleAt::At(DateTime::<Utc>::from(systemtime))
        }
        ScheduleAt::In(duration) => {
            use types::obelisk::types::time::Duration as WitDuration;
            let std_duration = match duration {
                WitDuration::Milliseconds(millis) => Duration::from_millis(millis),
                WitDuration::Seconds(secs) => Duration::from_secs(secs),
                WitDuration::Minutes(mins) => Duration::from_secs(u64::from(mins * 60)),
                WitDuration::Hours(hours) => Duration::from_secs(u64::from(hours * 60 * 60)),
                WitDuration::Days(days) => Duration::from_secs(u64::from(days * 24 * 60 * 60)),
            };
            HistoryEventScheduleAt::In(std_duration)
        }
    }
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
        runnable_component: RunnableComponent,
    ) -> Result<Self, WasmFileError> {
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
    pub fn link(
        self,
        engine: &Engine,
        fn_registry: &dyn FunctionRegistry,
    ) -> Result<WebhookEndpointInstanceLinked, WasmFileError> {
        let mut linker = Linker::new(engine);
        // Link wasi
        wasmtime_wasi::p2::add_to_linker_async(&mut linker)
            .map_err(|err| WasmFileError::linking_error("cannot link `wasmtime_wasi`", err))?;
        // Link wasi-http
        wasmtime_wasi_http::p2::add_only_http_to_linker_async(&mut linker)
            .map_err(|err| WasmFileError::linking_error("cannot link `wasmtime_wasi_http`", err))?;
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
            match linker.instance(import.ifc_fqn.deref()) {
                Ok(mut linker_instance) => {
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
                                WebhookEndpointCtx,
                            >,
                                  _component_func: ComponentFunc,
                                  params: &[Val],
                                  results: &mut [Val]| {
                                let ffqn = ffqn.clone();
                                let wasm_backtrace = if self.config.backtrace_persist {
                                    let wasm_backtrace =
                                        wasmtime::WasmBacktrace::capture(&store_ctx);
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
                            return Err(WasmFileError::linking_error(
                                format!("cannot add mock for imported function {ffqn}"),
                                err,
                            ));
                        }
                    }
                }
                Err(err) => {
                    warn!(
                        "Skipping interface {ifc_fqn} - {err:?}",
                        ifc_fqn = import.ifc_fqn
                    );
                }
            }
        }

        // Pre-instantiate to catch missing imports
        let proxy_pre = linker
            .instantiate_pre(&self.runnable_component.wasmtime_component)
            .map_err(|err: wasmtime::Error| {
                WasmFileError::linking_error("linking error while creating instantiate_pre", err)
            })?;
        let proxy_pre = Arc::new(ProxyPre::new(proxy_pre).map_err(|err: wasmtime::Error| {
            WasmFileError::linking_error("linking error while creating ProxyPre instance", err)
        })?);

        Ok(WebhookEndpointInstanceLinked {
            config: Arc::new(self.config),
            proxy_pre,
        })
    }
}

#[derive(Clone, derive_more::Debug)]
pub struct WebhookEndpointInstanceLinked {
    #[debug(skip)]
    proxy_pre: Arc<ProxyPre<WebhookEndpointCtx>>,
    config: Arc<WebhookEndpointConfig>,
}
impl WebhookEndpointInstanceLinked {
    #[must_use]
    pub fn build(
        &self,
        log_forwarder_sender: &mpsc::Sender<LogInfoAppendRow>,
    ) -> WebhookEndpointInstance {
        let stdout = StdOutputConfigWithSender::new(
            self.config.forward_stdout,
            log_forwarder_sender,
            LogStreamType::StdOut,
        );
        let stderr = StdOutputConfigWithSender::new(
            self.config.forward_stderr,
            log_forwarder_sender,
            LogStreamType::StdErr,
        );
        WebhookEndpointInstance {
            proxy_pre: self.proxy_pre.clone(),
            stdout,
            stderr,
            logs_storage_config: self.config.logs_store_min_level.map(|min_level| {
                LogStrageConfig {
                    min_level,
                    log_sender: log_forwarder_sender.clone(),
                }
            }),
            config: self.config.clone(),
        }
    }
}

#[derive(Clone, derive_more::Debug)]
pub struct WebhookEndpointInstance {
    #[debug(skip)]
    proxy_pre: Arc<ProxyPre<WebhookEndpointCtx>>,
    config: Arc<WebhookEndpointConfig>,
    #[debug(skip)]
    stdout: Option<StdOutputConfigWithSender>,
    #[debug(skip)]
    stderr: Option<StdOutputConfigWithSender>,
    logs_storage_config: Option<LogStrageConfig>,
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

/// Swappable per-server state for hot-redeploy.
pub struct WebhookServerState {
    pub deployment_id: DeploymentId,
    pub router: Arc<MethodAwareRouter<WebhookEndpointInstanceLinked>>,
    pub fn_registry: Arc<dyn FunctionRegistry>,
}

#[expect(clippy::too_many_arguments)]
pub async fn server(
    http_server: String,
    listener: TcpListener,
    engine: Arc<Engine>,
    wh_server_state_watcher_parent: WebhookStateWatcher,
    log_forwarder_sender: mpsc::Sender<LogInfoAppendRow>,
    db_pool: Arc<dyn DbPool>,
    clock_fn: Box<dyn ClockFn>,
    sleep: Arc<dyn Sleep>,
    max_inflight_requests: Option<Arc<tokio::sync::Semaphore>>,
    server_termination_watcher: watch::Receiver<()>,
) -> Result<(), WebhookServerError> {
    loop {
        let (stream, _) = listener
            .accept()
            .await
            .map_err(WebhookServerError::SocketError)?;
        let stream_id = format!("{stream:?}");
        let stream = TokioIo::new(stream);

        // Each connection must not affect other connections.
        let mut wh_server_state_watcher = wh_server_state_watcher_parent.clone();
        // Snapshot state before spawning, marking the latest value as seen.
        let state = wh_server_state_watcher.borrow_and_update().clone();
        let deployment_id = state.deployment_id;
        debug!(%deployment_id, %stream_id, "Initializing connection");
        // Spawn a tokio task for each TCP connection.
        tokio::task::spawn(
            {
                let engine = engine.clone();
                let clock_fn = clock_fn.clone_box();
                let sleep = sleep.clone();
                let db_pool = db_pool.clone();
                let http_server = http_server.clone();
                let connection_span = info_span!("connection", %http_server);
                let max_inflight_requests = max_inflight_requests.clone();
                let server_termination_watcher = server_termination_watcher.clone();
                let log_forwarder_sender = log_forwarder_sender.clone();
                async move {
                    let (connection_drop_sender, connection_drop_watcher) = watch::channel(());
                    let deployment_id = state.deployment_id;
                    let mut conn = http1::Builder::new()
                        .serve_connection(
                            stream,
                            hyper::service::service_fn({
                                move |req| {
                                    let execution_id = ExecutionId::generate().get_top_level();
                                    trace!(%execution_id, %deployment_id, method = %req.method(), uri = %req.uri(), "Processing request");
                                    RequestHandler {
                                        deployment_id: state.deployment_id,
                                        engine: engine.clone(),
                                        clock_fn: clock_fn.clone_box(),
                                        sleep: sleep.clone(),
                                        db_pool: db_pool.clone(),
                                        fn_registry: state.fn_registry.clone(),
                                        execution_id,
                                        router: state.router.clone(),
                                        connection_drop_watcher: connection_drop_watcher.clone(),
                                        server_termination_watcher: server_termination_watcher.clone(),
                                        log_forwarder_sender: log_forwarder_sender.clone()
                                    }
                                    .handle_request(req, max_inflight_requests.clone())
                                }.instrument(info_span!(parent: &connection_span, "request", %deployment_id))
                            })
                        );
                    let mut conn = Pin::new(&mut conn);
                    let res = loop {
                        select! {
                            result = conn.as_mut() => {
                                break result;
                            }
                            changed = wh_server_state_watcher.changed() => {
                                conn.as_mut().graceful_shutdown();
                                if changed.is_ok() {
                                    let new_deployment_id = wh_server_state_watcher.borrow().deployment_id;
                                    debug!(%http_server, "Switching to {new_deployment_id}, gracefully shutting down connection");
                                } else {
                                    debug!(%http_server, "Deployment watcher dropped, gracefully shutting down connection");
                                    break conn.as_mut().await;
                                }
                            }
                        }
                    };
                    if let Err(err) = res {
                        info!(%http_server, "Error serving connection: {err:?}");
                        drop(connection_drop_sender);
                    }
                }
            }.instrument(debug_span!("tcp stream", %deployment_id, %stream_id))
        );
    }
}

#[derive(Debug, Clone)]
pub struct WebhookEndpointConfig {
    pub component_id: ComponentId,
    pub forward_stdout: Option<StdOutputConfig>,
    pub forward_stderr: Option<StdOutputConfig>,
    pub env_vars: Arc<[EnvVar]>,
    pub fuel: Option<u64>,
    pub backtrace_persist: bool,
    pub subscription_interruption: Option<Duration>,
    pub logs_store_min_level: Option<LogLevel>,
    pub allowed_hosts: Arc<[crate::http_request_policy::AllowedHostConfig]>,
    pub js_config: Option<WebhookEndpointJsConfig>,
    /// The TOML config section type for error messages
    pub config_section_hint: crate::http_hooks::ConfigSectionHint,
}

#[derive(Debug, Clone)]
pub struct WebhookEndpointJsConfig {
    pub source: String,
    pub file_name: String,
}

struct WebhookEndpointCtx {
    component_id: ComponentId,
    deployment_id: DeploymentId,
    clock_fn: Box<dyn ClockFn>,
    sleep: Arc<dyn Sleep>,
    db_pool: Arc<dyn DbPool>,
    fn_registry: Arc<dyn FunctionRegistry>,
    table: ResourceTable,
    wasi_ctx: WasiCtx,
    http_ctx: WasiHttpCtx,
    execution_id: ExecutionIdTopLevel,
    next_join_set_idx: u64,
    version: Option<Version>,
    component_logger: ComponentLogger,
    subscription_interruption: Option<Duration>,
    connection_drop_watcher: watch::Receiver<()>,
    server_termination_watcher: watch::Receiver<()>,
    http_hooks: HttpHooks,
}

impl HostJoinSet for WebhookEndpointCtx {
    fn id(&mut self, _resource: wasmtime::component::Resource<JoinSetId>) -> String {
        unreachable!("webhook endpoint instances cannot obtain `join-set-id` resource")
    }

    fn submit_delay(
        &mut self,
        _self_: wasmtime::component::Resource<JoinSetId>,
        _timeout: types::obelisk::types::time::ScheduleAt,
    ) -> types::obelisk::types::execution::DelayId {
        unreachable!("webhook endpoint instances cannot obtain `join-set-id` resource")
    }

    fn join_next(
        &mut self,
        _self_: wasmtime::component::Resource<JoinSetId>,
    ) -> Result<(types::obelisk::types::execution::ResponseId, Result<(), ()>), JoinNextError> {
        unreachable!("webhook endpoint instances cannot obtain `join-set-id` resource")
    }

    fn drop(
        &mut self,
        _resource: wasmtime::component::Resource<JoinSetId>,
    ) -> wasmtime::Result<()> {
        unreachable!("webhook endpoint instances cannot obtain `join-set-id` resource")
    }
}

impl ExecutionHost for WebhookEndpointCtx {
    fn convert_schedule_json_error(
        &mut self,
        err: ScheduleJsonErrorTrappable,
    ) -> wasmtime::Result<types::obelisk::types::execution::ScheduleJsonError> {
        match err {
            ScheduleJsonErrorTrappable::Normal(err) => Ok(err),
            ScheduleJsonErrorTrappable::Trap(err) => Err(err),
        }
    }
}

fn wit_backtrace_to_storage(
    bt: types::obelisk::types::backtrace::WasmBacktrace,
) -> concepts::storage::WasmBacktrace {
    concepts::storage::WasmBacktrace {
        frames: bt
            .frames
            .into_iter()
            .map(|f| concepts::storage::FrameInfo {
                module: f.module,
                func_name: f.func_name,
                symbols: f
                    .symbols
                    .into_iter()
                    .map(|s| concepts::storage::FrameSymbol {
                        func_name: s.func_name,
                        file: s.file,
                        line: s.line,
                        col: s.col,
                    })
                    .collect(),
            })
            .collect(),
    }
}

impl WebhookSupportHost for WebhookEndpointCtx {
    async fn execution_id_generate(
        &mut self,
    ) -> wasmtime::Result<types::obelisk::webhook::webhook_support::ExecutionId> {
        let execution_id = ExecutionId::generate();
        Ok(types::obelisk::webhook::webhook_support::ExecutionId {
            id: execution_id.to_string(),
        })
    }

    async fn current_execution_id(
        &mut self,
    ) -> wasmtime::Result<types::obelisk::webhook::webhook_support::ExecutionId> {
        Ok(types::obelisk::webhook::webhook_support::ExecutionId {
            id: self.execution_id.to_string(),
        })
    }

    fn convert_get_status_error(
        &mut self,
        err: GetStatusErrorTrappable,
    ) -> wasmtime::Result<types::obelisk::webhook::webhook_support::GetStatusError> {
        match err {
            GetStatusErrorTrappable::Normal(err) => Ok(err),
            GetStatusErrorTrappable::Trap(err) => Err(err),
        }
    }

    fn convert_get_error(
        &mut self,
        err: GetErrorTrappable,
    ) -> wasmtime::Result<types::obelisk::webhook::webhook_support::GetError> {
        match err {
            GetErrorTrappable::Normal(err) => Ok(err),
            GetErrorTrappable::Trap(err) => Err(err),
        }
    }

    fn convert_try_get_error(
        &mut self,
        err: TryGetErrorTrappable,
    ) -> wasmtime::Result<types::obelisk::webhook::webhook_support::TryGetError> {
        match err {
            TryGetErrorTrappable::Normal(err) => Ok(err),
            TryGetErrorTrappable::Trap(err) => Err(err),
        }
    }

    async fn schedule_json(
        &mut self,
        execution_id: types::obelisk::webhook::webhook_support::ExecutionId,
        schedule_at: types::obelisk::webhook::webhook_support::ScheduleAt,
        function: types::obelisk::webhook::webhook_support::Function,
        params: String,
        _config: Option<types::obelisk::webhook::webhook_support::SubmitConfig>,
        backtrace: Option<types::obelisk::types::backtrace::WasmBacktrace>,
    ) -> Result<(), ScheduleJsonErrorTrappable> {
        use types::obelisk::types::execution::ScheduleJsonError;

        // Parse the execution ID
        let execution_id = match concepts::ExecutionId::from_str(&execution_id.id) {
            Ok(id) => id,
            Err(err) => {
                let msg = format!("schedule-json: invalid execution ID: {err}");
                self.error(msg.clone());
                return Err(ScheduleJsonError::FfqnParsingError(msg).into());
            }
        };

        // Parse the function FFQN
        let ffqn =
            match FunctionFqn::try_from_tuple(&function.interface_name, &function.function_name) {
                Ok(ffqn) => ffqn,
                Err(err) => {
                    let msg = format!("schedule-json: invalid function name: {err}");
                    self.error(msg.clone());
                    return Err(ScheduleJsonError::FfqnParsingError(msg).into());
                }
            };

        // Look up function in registry
        let Some((fn_metadata, component_id)) = self.fn_registry.get_by_exported_function(&ffqn)
        else {
            self.error("schedule-json: function not found".to_string());
            return Err(ScheduleJsonError::FunctionNotFound.into());
        };

        // Parse params JSON array
        let params_json: Vec<serde_json::Value> = match serde_json::from_str(&params) {
            Ok(serde_json::Value::Array(arr)) => arr,
            Ok(_) => {
                let msg = "schedule-json: params must be a JSON array".to_string();
                self.error(msg.clone());
                return Err(ScheduleJsonError::TypeCheckError(msg).into());
            }
            Err(err) => {
                let msg = format!("schedule-json: cannot parse params as JSON: {err}");
                self.error(msg.clone());
                return Err(ScheduleJsonError::TypeCheckError(msg).into());
            }
        };

        // Type check and convert params
        let params = match Params::from_json_values(
            Arc::from(params_json),
            fn_metadata
                .parameter_types
                .iter()
                .map(|param_type| &param_type.type_wrapper),
        ) {
            Ok(params) => params,
            Err(err) => {
                let msg = format!("schedule-json: params type checking failed: {err}");
                self.error(msg.clone());
                return Err(ScheduleJsonError::TypeCheckError(msg).into());
            }
        };

        // Convert schedule_at
        let history_event_schedule_at = schedule_at_from_webhook(schedule_at);
        let created_at = self.clock_fn.now();
        let schedule_at = match history_event_schedule_at.as_date_time(created_at) {
            Ok(dt) => dt,
            Err(err) => {
                let msg = format!("schedule-json: invalid schedule-at: {err}");
                self.error(msg.clone());
                return Err(ScheduleJsonError::TypeCheckError(msg).into());
            }
        };

        // Create execution in database
        let version = match self.get_version_or_create().await {
            Ok(v) => v,
            Err(err) => {
                return Err(wasmtime::Error::msg(format!("database error: {err:?}")).into());
            }
        };

        let event = HistoryEvent::Schedule {
            execution_id: execution_id.clone(),
            schedule_at: history_event_schedule_at,
            result: Ok(()),
        };
        let append_req = AppendRequest {
            event: ExecutionRequest::HistoryEvent { event },
            created_at,
        };
        let create_req = CreateRequest {
            created_at,
            execution_id: execution_id.clone(),
            ffqn,
            params,
            parent: None,
            metadata: ExecutionMetadata::from_linked_span(&self.component_logger.span),
            scheduled_at: schedule_at,
            component_id: component_id.clone(),
            deployment_id: self.deployment_id,
            scheduled_by: Some(ExecutionId::TopLevel(self.execution_id)),
            paused: false,
        };

        let db_connection = match self.db_pool.connection().await {
            Ok(conn) => conn,
            Err(err) => {
                return Err(
                    wasmtime::Error::msg(format!("database connection error: {err:?}")).into(),
                );
            }
        };

        let backtrace_infos: Vec<BacktraceInfo> = backtrace
            .map(|bt| BacktraceInfo {
                execution_id: ExecutionId::TopLevel(self.execution_id),
                component_id: self.component_id.clone(),
                version_min_including: version.clone(),
                version_max_excluding: Version(version.0 + 1),
                wasm_backtrace: wit_backtrace_to_storage(bt),
            })
            .into_iter()
            .collect();

        match db_connection
            .append_batch_create_new_execution(
                created_at,
                vec![append_req],
                ExecutionId::TopLevel(self.execution_id),
                version.clone(),
                vec![create_req],
                backtrace_infos,
            )
            .await
        {
            Ok(new_version) => {
                self.version = Some(new_version);
                Ok(())
            }
            Err(err) => Err(wasmtime::Error::msg(format!("database write error: {err:?}")).into()),
        }
    }

    async fn call_json(
        &mut self,
        function: types::obelisk::webhook::webhook_support::Function,
        params: String,
        _config: Option<types::obelisk::webhook::webhook_support::SubmitConfig>,
        backtrace: Option<types::obelisk::types::backtrace::WasmBacktrace>,
    ) -> Result<Result<Option<String>, Option<String>>, ScheduleJsonErrorTrappable> {
        use types::obelisk::types::execution::ScheduleJsonError;

        // Parse the function FFQN
        let ffqn =
            match FunctionFqn::try_from_tuple(&function.interface_name, &function.function_name) {
                Ok(ffqn) => ffqn,
                Err(err) => {
                    let msg = format!("call-json: invalid function name: {err}");
                    self.error(msg.clone());
                    return Err(ScheduleJsonError::FfqnParsingError(msg).into());
                }
            };

        // Look up function in registry
        let Some((fn_metadata, component_id)) = self.fn_registry.get_by_exported_function(&ffqn)
        else {
            self.error("call-json: function not found".to_string());
            return Err(ScheduleJsonError::FunctionNotFound.into());
        };

        // Parse params JSON array
        let params_json: Vec<serde_json::Value> = match serde_json::from_str(&params) {
            Ok(serde_json::Value::Array(arr)) => arr,
            Ok(_) => {
                let msg = "call-json: params must be a JSON array".to_string();
                self.error(msg.clone());
                return Err(ScheduleJsonError::TypeCheckError(msg).into());
            }
            Err(err) => {
                let msg = format!("call-json: cannot parse params as JSON: {err}");
                self.error(msg.clone());
                return Err(ScheduleJsonError::TypeCheckError(msg).into());
            }
        };

        // Type check and convert params
        let params = match Params::from_json_values(
            Arc::from(params_json),
            fn_metadata
                .parameter_types
                .iter()
                .map(|param_type| &param_type.type_wrapper),
        ) {
            Ok(params) => params,
            Err(err) => {
                let msg = format!("call-json: params type checking failed: {err}");
                self.error(msg.clone());
                return Err(ScheduleJsonError::TypeCheckError(msg).into());
            }
        };

        // Get or create version
        let version = match self.get_version_or_create().await {
            Ok(v) => v,
            Err(err) => {
                return Err(wasmtime::Error::msg(format!("database error: {err:?}")).into());
            }
        };

        // Create a OneOff join set and child execution ID
        let (join_set_id, child_execution_id) = self.create_oneoff_join_set();

        let created_at = self.clock_fn.now();

        // 1. Create join set
        let req_join_set_created = AppendRequest {
            created_at,
            event: ExecutionRequest::HistoryEvent {
                event: HistoryEvent::JoinSetCreate {
                    join_set_id: join_set_id.clone(),
                },
            },
        };

        // 2. Create child execution request
        let req_child_exec = AppendRequest {
            created_at,
            event: ExecutionRequest::HistoryEvent {
                event: HistoryEvent::JoinSetRequest {
                    join_set_id: join_set_id.clone(),
                    request: JoinSetRequest::ChildExecutionRequest {
                        child_execution_id: child_execution_id.clone(),
                        target_ffqn: ffqn.clone(),
                        params: params.clone(),
                        result: Ok(()),
                    },
                },
            },
        };

        // 3. Add JoinNext to wait for result
        let req_join_next = AppendRequest {
            created_at,
            event: ExecutionRequest::HistoryEvent {
                event: HistoryEvent::JoinNext {
                    join_set_id: join_set_id.clone(),
                    run_expires_at: created_at,
                    closing: false,
                    requested_ffqn: Some(ffqn.clone()),
                },
            },
        };

        // Create the child execution
        let req_create_child = CreateRequest {
            created_at,
            execution_id: ExecutionId::Derived(child_execution_id.clone()),
            ffqn,
            params,
            parent: Some((ExecutionId::TopLevel(self.execution_id), join_set_id)),
            metadata: ExecutionMetadata::from_parent_span(&self.component_logger.span),
            scheduled_at: created_at,
            component_id: component_id.clone(),
            deployment_id: self.deployment_id,
            scheduled_by: None,
            paused: false,
        };

        let db_connection = match self.db_pool.connection().await {
            Ok(conn) => conn,
            Err(err) => {
                return Err(
                    wasmtime::Error::msg(format!("database connection error: {err:?}")).into(),
                );
            }
        };

        let appended = vec![req_join_set_created, req_child_exec, req_join_next];

        let backtrace_infos: Vec<BacktraceInfo> = backtrace
            .map(|bt| BacktraceInfo {
                execution_id: ExecutionId::TopLevel(self.execution_id),
                component_id: self.component_id.clone(),
                version_min_including: version.clone(),
                version_max_excluding: Version(version.0 + 3),
                wasm_backtrace: wit_backtrace_to_storage(bt),
            })
            .into_iter()
            .collect();

        match db_connection
            .append_batch_create_new_execution(
                created_at,
                appended,
                ExecutionId::TopLevel(self.execution_id),
                version,
                vec![req_create_child],
                backtrace_infos,
            )
            .await
        {
            Ok(new_version) => {
                self.version = Some(new_version);
            }
            Err(err) => {
                return Err(wasmtime::Error::msg(format!("database write error: {err:?}")).into());
            }
        }

        // Wait for the result
        let result = Self::wait_for_finished_result(
            self.subscription_interruption,
            &self.sleep,
            db_connection.as_ref(),
            &ExecutionId::Derived(child_execution_id),
            &self.connection_drop_watcher,
            &self.server_termination_watcher,
        )
        .await;

        match result {
            Ok(retval) => Ok(supported_return_value_to_json_result(&retval)),
            Err(err) => Err(wasmtime::Error::msg(format!("execution error: {err:?}")).into()),
        }
    }

    async fn get_status(
        &mut self,
        execution_id: types::obelisk::webhook::webhook_support::ExecutionId,
        _backtrace: Option<types::obelisk::types::backtrace::WasmBacktrace>,
    ) -> Result<types::obelisk::webhook::webhook_support::ExecutionStatus, GetStatusErrorTrappable>
    {
        use types::obelisk::webhook::webhook_support::{
            ExecutionStatus, ExecutionStatusFinished, GetStatusError,
        };

        // Parse the execution ID
        let execution_id = match concepts::ExecutionId::from_str(&execution_id.id) {
            Ok(id) => id,
            Err(err) => {
                let msg = format!("get-status: cannot parse execution ID: {err}");
                self.error(msg.clone());
                return Err(GetStatusError::ExecutionIdParsingError(msg).into());
            }
        };

        // Get execution status from database
        let db_connection = match self.db_pool.connection().await {
            Ok(conn) => conn,
            Err(err) => {
                return Err(
                    wasmtime::Error::msg(format!("database connection error: {err:?}")).into(),
                );
            }
        };

        let execution_with_state = match db_connection.get_pending_state(&execution_id).await {
            Ok(state) => state,
            Err(DbErrorRead::NotFound) => {
                return Err(GetStatusError::NotFound.into());
            }
            Err(err) => {
                return Err(wasmtime::Error::msg(format!("database read error: {err:?}")).into());
            }
        };

        // Convert PendingState to ExecutionStatus
        let status = match execution_with_state.pending_state {
            PendingState::PendingAt(state) => {
                ExecutionStatus::PendingAt(types::obelisk::types::time::Datetime {
                    seconds: u64::try_from(state.scheduled_at.timestamp())
                        .expect("pending at before unix epoch is unsupported"),
                    nanoseconds: state.scheduled_at.timestamp_subsec_nanos(),
                })
            }
            PendingState::Locked(_) => ExecutionStatus::Locked,
            PendingState::BlockedByJoinSet(_) => ExecutionStatus::BlockedByJoinSet,
            PendingState::Paused(_) => ExecutionStatus::Paused,
            PendingState::Finished(finished) => {
                let finished_status = match finished.result_kind {
                    PendingStateFinishedResultKind::Ok => ExecutionStatusFinished::Ok,
                    PendingStateFinishedResultKind::Err(PendingStateFinishedError::Error) => {
                        ExecutionStatusFinished::Err
                    }
                    PendingStateFinishedResultKind::Err(
                        PendingStateFinishedError::ExecutionFailure(_),
                    ) => ExecutionStatusFinished::ExecutionFailure,
                };
                ExecutionStatus::Finished(finished_status)
            }
        };

        Ok(status)
    }

    async fn get(
        &mut self,
        execution_id: types::obelisk::webhook::webhook_support::ExecutionId,
        _backtrace: Option<types::obelisk::types::backtrace::WasmBacktrace>,
    ) -> Result<Result<Option<String>, Option<String>>, GetErrorTrappable> {
        use types::obelisk::webhook::webhook_support::GetError;

        // Parse the execution ID
        let parsed_execution_id: concepts::ExecutionId =
            match concepts::ExecutionId::from_str(&execution_id.id) {
                Ok(id) => id,
                Err(err) => {
                    let msg = format!("get: cannot parse execution ID: {err}");
                    self.error(msg.clone());
                    return Err(GetError::ExecutionIdParsingError(msg).into());
                }
            };

        // Get database connection
        let db_connection = match self.db_pool.connection().await {
            Ok(conn) => conn,
            Err(err) => {
                return Err(
                    wasmtime::Error::msg(format!("database connection error: {err:?}")).into(),
                );
            }
        };

        // Extract needed values before the async call to avoid borrowing issues
        let subscription_interruption = self.subscription_interruption;
        let sleep = self.sleep.clone();
        let connection_drop_watcher = self.connection_drop_watcher.clone();
        let server_termination_watcher = self.server_termination_watcher.clone();

        // Wait for the execution to finish
        let result = Self::wait_for_finished_result(
            subscription_interruption,
            &sleep,
            db_connection.as_ref(),
            &parsed_execution_id,
            &connection_drop_watcher,
            &server_termination_watcher,
        )
        .await;

        match result {
            Ok(retval) => Ok(supported_return_value_to_json_result(&retval)),
            Err(err) => Err(wasmtime::Error::msg(format!("execution error: {err:?}")).into()),
        }
    }

    async fn try_get(
        &mut self,
        execution_id: types::obelisk::webhook::webhook_support::ExecutionId,
        _backtrace: Option<types::obelisk::types::backtrace::WasmBacktrace>,
    ) -> Result<Result<Option<String>, Option<String>>, TryGetErrorTrappable> {
        use types::obelisk::webhook::webhook_support::TryGetError;

        // Parse the execution ID
        let parsed_execution_id: concepts::ExecutionId =
            match concepts::ExecutionId::from_str(&execution_id.id) {
                Ok(id) => id,
                Err(err) => {
                    let msg = format!("try-get: cannot parse execution ID: {err}");
                    self.error(msg.clone());
                    return Err(TryGetError::ExecutionIdParsingError(msg).into());
                }
            };

        // Get database connection
        let db_connection = match self.db_pool.connection().await {
            Ok(conn) => conn,
            Err(err) => {
                return Err(
                    wasmtime::Error::msg(format!("database connection error: {err:?}")).into(),
                );
            }
        };

        // Get execution state
        let execution_with_state = match db_connection.get_pending_state(&parsed_execution_id).await
        {
            Ok(state) => state,
            Err(DbErrorRead::NotFound) => {
                self.error(format!("try-get: execution not found: {}", execution_id.id));
                return Err(TryGetError::NotFound.into());
            }
            Err(err) => {
                return Err(wasmtime::Error::msg(format!("database read error: {err:?}")).into());
            }
        };

        // Check if finished
        match execution_with_state.pending_state {
            PendingState::Finished(_) => {
                // Get the actual result
                match db_connection
                    .wait_for_finished_result(&parsed_execution_id, None)
                    .await
                {
                    Ok(retval) => Ok(supported_return_value_to_json_result(&retval)),
                    Err(err) => {
                        Err(wasmtime::Error::msg(format!("database read error: {err:?}")).into())
                    }
                }
            }
            _ => Err(TryGetError::NotFinishedYet.into()),
        }
    }
}

#[derive(thiserror::Error, Debug, Clone)]

enum WebhookEndpointFunctionError {
    #[error(transparent)]
    DbError(#[from] DbErrorWrite),
    #[error(transparent)]
    FinishedExecutionError(#[from] FinishedExecutionError),
    #[error("uncategorized error: {0}")]
    UncategorizedError(&'static str),
    #[error("connection closed")]
    ConnectionClosed,
}
impl From<DbErrorGeneric> for WebhookEndpointFunctionError {
    fn from(value: DbErrorGeneric) -> Self {
        WebhookEndpointFunctionError::DbError(DbErrorWrite::Generic(value))
    }
}

impl wasmtime::component::HasData for WebhookEndpointCtx {
    type Data<'a> = &'a mut WebhookEndpointCtx;
}

impl WebhookEndpointCtx {
    // Create new execution if this is the first call of the request/response cycle
    async fn get_version_or_create(&mut self) -> Result<Version, DbErrorWrite> {
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
            component_id: self.component_id.clone(),
            deployment_id: self.deployment_id,
            scheduled_by: None,
            paused: false,
        };
        let conn = self.db_pool.connection().await?;
        let version = conn.create(create_request).await?;
        self.version = Some(version.clone());
        Ok(version)
    }

    /// Create a new `OneOff` join set and return its ID along with a child execution ID.
    fn create_oneoff_join_set(&mut self) -> (JoinSetId, ExecutionIdDerived) {
        let join_set_id = JoinSetId::new(
            JoinSetKind::OneOff,
            StrVariant::from(self.next_join_set_idx.to_string()),
        )
        .expect("numeric names must be allowed");
        self.next_join_set_idx += 1;
        let child_execution_id = ExecutionId::TopLevel(self.execution_id).next_level(&join_set_id);
        (join_set_id, child_execution_id)
    }

    #[instrument(skip_all, fields(%ffqn, version, %execution_id = self.execution_id))]
    async fn call_imported_fn(
        &mut self,
        ffqn: FunctionFqn,
        params: &[Val],
        results: &mut [Val],
        wasm_backtrace: Option<concepts::storage::WasmBacktrace>,
    ) -> Result<(), WebhookEndpointFunctionError> {
        trace!(?params, "call_imported_fn start");
        assert_eq!(
            1,
            results.len(),
            "direct call: no-ext export must return `result`, -schedule returns `execuiton-id`"
        );

        if self.connection_drop_watcher.has_changed().is_err()
            || self.server_termination_watcher.has_changed().is_err()
        {
            debug!("Cancellation request detected");
            return Err(WebhookEndpointFunctionError::ConnectionClosed);
        }

        if let Some(package_name) = ffqn.ifc_fqn.package_strip_obelisk_schedule_suffix() {
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
                let version = self.get_version_or_create().await?;
                let span = Span::current();
                span.record("version", tracing::field::display(&version));
                let new_execution_id = ExecutionId::generate();
                let (_function_metadata, child_component_id) = self
                    .fn_registry
                    .get_by_exported_function(&ffqn)
                    .expect("target function must be found in fn_registry");
                let created_at = self.clock_fn.now();

                let event = HistoryEvent::Schedule {
                    execution_id: new_execution_id.clone(),
                    schedule_at,
                    result: Ok(()),
                };
                let schedule_at = schedule_at.as_date_time(created_at).map_err(|_err| {
                    WebhookEndpointFunctionError::UncategorizedError("schedule-at conversion error")
                })?;
                let child_exec_req = AppendRequest {
                    event: ExecutionRequest::HistoryEvent { event },
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
                    component_id: child_component_id.clone(),
                    deployment_id: self.deployment_id,
                    scheduled_by: Some(ExecutionId::TopLevel(self.execution_id)),
                    paused: false,
                };
                let db_connection = self.db_pool.connection().await?;
                let expected_next_version = version.increment();
                let backtrace_info = wasm_backtrace.map(|wasm_backtrace| BacktraceInfo {
                    execution_id: ExecutionId::TopLevel(self.execution_id),
                    component_id: self.component_id.clone(),
                    version_min_including: version.clone(),
                    version_max_excluding: expected_next_version.clone(),
                    wasm_backtrace,
                });
                let version = db_connection
                    .append_batch_create_new_execution(
                        created_at,
                        vec![child_exec_req],
                        ExecutionId::TopLevel(self.execution_id),
                        version.clone(),
                        vec![create_child_req],
                        backtrace_info.into_iter().collect(),
                    )
                    .await?;
                assert_eq!(version, expected_next_version); // Expected for backtrace's version_max_excluding
                self.version = Some(version.clone());
                results[0] = execution_id_into_val(&new_execution_id);
            } else {
                error!("unrecognized `{SUFFIX_PKG_SCHEDULE}` extension function {ffqn}");
                return Err(WebhookEndpointFunctionError::UncategorizedError(
                    "unrecognized extension function",
                ));
            }
        } else {
            // direct call
            let version = self.get_version_or_create().await?;
            let span = Span::current();
            span.record("version", tracing::field::display(&version));
            let (join_set_id_direct, child_execution_id) = self.create_oneoff_join_set();
            let created_at = self.clock_fn.now();
            let (fn_metadata, child_component_id) = self
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
                event: ExecutionRequest::HistoryEvent {
                    event: HistoryEvent::JoinSetCreate {
                        join_set_id: join_set_id_direct.clone(),
                    },
                },
            };
            let params = Params::from_wasmtime(Arc::from(params));
            let req_child_exec = AppendRequest {
                created_at,
                event: ExecutionRequest::HistoryEvent {
                    event: HistoryEvent::JoinSetRequest {
                        join_set_id: join_set_id_direct.clone(),
                        request: JoinSetRequest::ChildExecutionRequest {
                            child_execution_id: child_execution_id.clone(),
                            target_ffqn: ffqn.clone(),
                            params: params.clone(),
                            result: Ok(()),
                        },
                    },
                },
            };
            let req_join_next = AppendRequest {
                created_at,
                event: ExecutionRequest::HistoryEvent {
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
                params,
                parent: Some((ExecutionId::TopLevel(self.execution_id), join_set_id_direct)),
                metadata: ExecutionMetadata::from_parent_span(&self.component_logger.span),
                scheduled_at: created_at,
                component_id: child_component_id.clone(),
                deployment_id: self.deployment_id,
                scheduled_by: None,
                paused: false,
            };
            let db_connection = self.db_pool.connection().await?;
            let appended = vec![req_join_set_created, req_child_exec, req_join_next];
            let expected_next_version = Version(version.0 + 3);
            let backtrace_info = wasm_backtrace.map(|wasm_backtrace| BacktraceInfo {
                execution_id: ExecutionId::TopLevel(self.execution_id),
                component_id: self.component_id.clone(),
                version_min_including: version.clone(),
                version_max_excluding: expected_next_version.clone(),
                wasm_backtrace,
            });

            let version = db_connection
                .append_batch_create_new_execution(
                    created_at,
                    appended,
                    ExecutionId::TopLevel(self.execution_id),
                    version,
                    vec![req_create_child],
                    backtrace_info.into_iter().collect(),
                )
                .await?;
            assert_eq!(version, expected_next_version); // Expected for backtrace's version_max_excluding
            self.version = Some(version);

            let res = Self::wait_for_finished_result(
                self.subscription_interruption,
                &self.sleep,
                db_connection.as_ref(),
                &ExecutionId::Derived(child_execution_id),
                &self.connection_drop_watcher,
                &self.server_termination_watcher,
            )
            .await?;
            results[0] = res.into_wast_val(move || return_type_tl).as_val();

            trace!(?results, "call_imported_fn finish");
        }
        Ok(())
    }

    async fn wait_for_finished_result(
        subscription_interruption: Option<Duration>,
        sleep: &Arc<dyn Sleep>,
        db_connection: &dyn DbConnection,
        execution_id: &ExecutionId,
        connection_drop_watcher: &watch::Receiver<()>,
        server_termination_watcher: &watch::Receiver<()>,
    ) -> Result<SupportedFunctionReturnValue, WebhookEndpointFunctionError> {
        let timeout_factory = move || {
            let subscription_interruption = subscription_interruption.unwrap_or(Duration::MAX);
            let sleep = sleep.clone();
            let mut connection_drop_watcher = connection_drop_watcher.clone();
            let mut server_termination_watcher = server_termination_watcher.clone();
            Box::pin(async move {
                select! {
                    () = sleep.sleep(subscription_interruption) => TimeoutOutcome::Timeout,
                    _ = connection_drop_watcher.changed() => TimeoutOutcome::Cancel,
                    _ = server_termination_watcher.changed() => TimeoutOutcome::Cancel,
                }
            })
        };

        loop {
            let timeout = timeout_factory();
            let res = db_connection
                .wait_for_finished_result(execution_id, Some(timeout))
                .await;
            match res {
                Ok(ok) => {
                    trace!("Finished ok");
                    return Ok(ok);
                }
                Err(DbErrorReadWithTimeout::Timeout(TimeoutOutcome::Timeout)) => {
                    trace!("Timeout triggers resubscribing");
                }
                Err(DbErrorReadWithTimeout::Timeout(TimeoutOutcome::Cancel)) => {
                    debug!("Connection closed, not waiting for result");
                    return Err(WebhookEndpointFunctionError::ConnectionClosed);
                }
                Err(DbErrorReadWithTimeout::DbErrorRead(err)) => {
                    warn!("Database error: {err:?}");
                    return Err(WebhookEndpointFunctionError::from(DbErrorWrite::from(err)));
                }
            }
        }
    }

    fn add_to_linker(linker: &mut Linker<WebhookEndpointCtx>) -> Result<(), WasmFileError> {
        // link obelisk:log
        log_activities::obelisk::log::log::add_to_linker::<_, WebhookEndpointCtx>(linker, |x| x)
            .map_err(|err| WasmFileError::linking_error("cannot link log activities", err))?;
        // link obelisk:types
        types::obelisk::types::execution::add_to_linker::<_, WebhookEndpointCtx>(linker, |x| x)
            .map_err(|err| WasmFileError::linking_error("cannot link obelisk:types", err))?;
        // link obelisk:webhook/webhook-support
        types::obelisk::webhook::webhook_support::add_to_linker::<_, WebhookEndpointCtx>(
            linker,
            |x| x,
        )
        .map_err(|err| WasmFileError::linking_error("cannot link obelisk:webhook", err))?;
        Ok(())
    }

    #[must_use]
    #[expect(clippy::too_many_arguments)]
    fn new<'a>(
        deployment_id: DeploymentId,
        config: &Arc<WebhookEndpointConfig>,
        engine: &Engine,
        clock_fn: Box<dyn ClockFn>,
        sleep: Arc<dyn Sleep>,
        db_pool: Arc<dyn DbPool>,
        fn_registry: Arc<dyn FunctionRegistry>,
        params: impl Iterator<Item = (&'a str, &'a str)>,
        execution_id: ExecutionIdTopLevel,
        request_span: Span,
        connection_drop_watcher: watch::Receiver<()>,
        server_termination_watcher: watch::Receiver<()>,
        stdout: Option<StdOutput>,
        stderr: Option<StdOutput>,
        run_id: RunId,
        logs_storage_config: Option<LogStrageConfig>,
    ) -> Store<WebhookEndpointCtx> {
        let mut wasi_ctx = WasiCtxBuilder::new();
        if let Some(stdout) = stdout {
            let stdout = LogStream::new(
                format!(
                    "[{component_id} {execution_id} stdout]",
                    component_id = config.component_id
                ),
                stdout,
            );
            wasi_ctx.stdout(stdout);
        }
        if let Some(stderr) = stderr {
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
        if let Some(js_config) = &config.js_config {
            wasi_ctx.env("__OBELISK_JS_SOURCE__", &js_config.source);
            wasi_ctx.env("__OBELISK_JS_FILE_NAME__", &js_config.file_name);
        }

        // Generate fresh placeholders for this execution run
        let http_policy =
            crate::policy_builder::build_http_policy(&config.allowed_hosts, &mut wasi_ctx);

        for (key, val) in params {
            wasi_ctx.env(key, val);
        }
        let wasi_ctx = wasi_ctx.build();
        let component_logger = ComponentLogger {
            span: request_span,
            execution_id: ExecutionId::TopLevel(execution_id),
            run_id,
            logs_storage_config,
        };
        // All child executions are part of the same join set.
        let ctx = WebhookEndpointCtx {
            clock_fn: clock_fn.clone_box(),
            sleep,
            db_pool,
            fn_registry,
            table: ResourceTable::new(),
            wasi_ctx,
            http_ctx: WasiHttpCtx::new(),
            version: None,
            component_id: config.component_id.clone(),
            deployment_id,
            next_join_set_idx: JOIN_SET_START_IDX,
            execution_id,
            component_logger: component_logger.clone(),
            subscription_interruption: config.subscription_interruption,
            connection_drop_watcher,
            server_termination_watcher,
            http_hooks: HttpHooks {
                clock_fn,
                http_client_traces: HttpClientTracesContainer::default(),
                http_policy,
                component_logger,
                config_section_hint: config.config_section_hint,
            },
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

                SupportedFunctionReturnValue::ExecutionError(FinishedExecutionError {
                    reason: Some(err.to_string()),
                    kind: ExecutionFailureKind::Uncategorized,
                    detail: err.detail,
                })
            }
        };
        if let Some(version) = self.version {
            let http_client_traces = Some(
                self.http_hooks
                    .http_client_traces
                    .into_iter()
                    .map(|(req, mut resp)| HttpClientTrace {
                        req,
                        resp: resp.try_recv().ok(),
                    })
                    .collect(),
            );
            self.db_pool
                .connection()
                .await?
                .append(
                    ExecutionId::TopLevel(self.execution_id),
                    version,
                    AppendRequest {
                        created_at: self.clock_fn.now(),
                        event: ExecutionRequest::Finished {
                            retval: result,
                            http_client_traces,
                        },
                    },
                )
                .await?;
        }
        original_result
    }
}

impl log_activities::obelisk::log::log::Host for WebhookEndpointCtx {
    fn trace(&mut self, message: String) {
        self.component_logger.log(LogLevel::Trace, message);
    }

    fn debug(&mut self, message: String) {
        self.component_logger.log(LogLevel::Debug, message);
    }

    fn info(&mut self, message: String) {
        self.component_logger.log(LogLevel::Info, message);
    }

    fn warn(&mut self, message: String) {
        self.component_logger.log(LogLevel::Warn, message);
    }

    fn error(&mut self, message: String) {
        self.component_logger.log(LogLevel::Error, message);
    }
}

impl WasiView for WebhookEndpointCtx {
    fn ctx(&mut self) -> WasiCtxView<'_> {
        WasiCtxView {
            ctx: &mut self.wasi_ctx,
            table: &mut self.table,
        }
    }
}
impl IoView for WebhookEndpointCtx {
    fn table(&mut self) -> &mut ResourceTable {
        &mut self.table
    }
}
impl WasiHttpView for WebhookEndpointCtx {
    fn http(&mut self) -> WasiHttpCtxView<'_> {
        WasiHttpCtxView {
            ctx: &mut self.http_ctx,
            table: &mut self.table,
            hooks: &mut self.http_hooks,
        }
    }
}

struct RequestHandler {
    deployment_id: DeploymentId,
    engine: Arc<Engine>,
    clock_fn: Box<dyn ClockFn>,
    sleep: Arc<dyn Sleep>,
    db_pool: Arc<dyn DbPool>,
    fn_registry: Arc<dyn FunctionRegistry>,
    execution_id: ExecutionIdTopLevel,
    router: Arc<MethodAwareRouter<WebhookEndpointInstanceLinked>>,
    connection_drop_watcher: watch::Receiver<()>,
    server_termination_watcher: watch::Receiver<()>,
    log_forwarder_sender: mpsc::Sender<LogInfoAppendRow>,
}

fn respond(body: &str, status_code: StatusCode) -> hyper::Response<HyperOutgoingBody> {
    let body = UnsyncBoxBody::new(http_body_util::BodyExt::map_err(
        http_body_util::Full::new(Bytes::copy_from_slice(body.as_bytes())),
        |_| unreachable!(),
    ));
    hyper::Response::builder()
        .status(status_code)
        .body(body)
        .unwrap()
}

impl RequestHandler {
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
            return Ok::<_, hyper::Error>(respond("Out of permits", StatusCode::TOO_MANY_REQUESTS));
        };

        let res = self
            .handle_request_inner(req, http_request_guard, Span::current())
            .await;
        match res {
            Ok(body) => Ok(body),
            Err(err) => {
                debug!("{err:?}");
                Ok(match err {
                    HandleRequestError::IncomingRequestError(err) => respond(
                        &format!("Incoming request error: {err}"),
                        StatusCode::BAD_REQUEST,
                    ),
                    HandleRequestError::ResponseCreationError(err) => respond(
                        &format!("Cannot create response: {err}"),
                        StatusCode::INTERNAL_SERVER_ERROR,
                    ),
                    HandleRequestError::InstantiationError(err) => respond(
                        &format!("Cannot instantiate: {err}"),
                        StatusCode::SERVICE_UNAVAILABLE,
                    ),
                    HandleRequestError::ErrorCode(code) => respond(
                        &format!("Error code: {code}"),
                        StatusCode::INTERNAL_SERVER_ERROR,
                    ),
                    HandleRequestError::ExecutionError(_) => {
                        respond("Component Error", StatusCode::INTERNAL_SERVER_ERROR)
                    }
                    HandleRequestError::RouteNotFound => {
                        respond("Route not found", StatusCode::NOT_FOUND)
                    }
                    HandleRequestError::Timeout => respond("Timeout", StatusCode::REQUEST_TIMEOUT),
                    HandleRequestError::InstanceLimitReached => {
                        respond("Instance limit reached", StatusCode::SERVICE_UNAVAILABLE)
                    }
                })
            }
        }
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

        if let Some(instance_match) = self.router.find(req.method(), req.uri()) {
            let found_instance = instance_match.handler();
            let found_instance = found_instance.build(&self.log_forwarder_sender);
            let run_id = RunId::generate();
            let stdout = found_instance.stdout.as_ref().map(|stdoutput| {
                stdoutput.build(&ExecutionId::TopLevel(self.execution_id), run_id)
            });
            let stderr = found_instance.stderr.as_ref().map(|stdoutput| {
                stdoutput.build(&ExecutionId::TopLevel(self.execution_id), run_id)
            });
            let (sender, receiver) = tokio::sync::oneshot::channel();
            let mut store = WebhookEndpointCtx::new(
                self.deployment_id,
                &found_instance.config,
                &self.engine,
                self.clock_fn,
                self.sleep,
                self.db_pool,
                self.fn_registry,
                instance_match.params().iter(),
                self.execution_id,
                request_span.clone(),
                self.connection_drop_watcher,
                self.server_termination_watcher,
                stdout,
                stderr,
                run_id,
                found_instance.logs_storage_config.clone(),
            );
            let req = store
                .data_mut()
                .http()
                .new_incoming_request(Scheme::Http, req)
                .map_err(|err| HandleRequestError::IncomingRequestError(err.into()))?;
            let out = store
                .data_mut()
                .http()
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
                        .inspect_err(|err| debug!("Webhook instance returned error: {err:?}"));
                    let ctx = store.into_data();
                    ctx.close(result, assigned_fuel).await
                }
                .instrument(request_span)
            });
            match receiver.await {
                Ok(Ok(resp)) => {
                    trace!("Streaming the response");
                    Ok(resp)
                }
                Ok(Err(err)) => {
                    debug!("Webhook instance sent error code {err:?}");
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
    ErrorCode(wasmtime_wasi_http::p2::bindings::http::types::ErrorCode),
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
    use crate::{
        RunnableComponent,
        engines::{EngineConfig, Engines},
    };

    use super::MethodAwareRouter;
    use assert_matches::assert_matches;
    use concepts::ComponentType;
    use hyper::{Method, Uri};

    pub(crate) fn compile_webhook(wasm_path: &str) -> RunnableComponent {
        let engine = Engines::get_webhook_engine(EngineConfig::on_demand_testing()).unwrap();
        RunnableComponent::new(wasm_path, &engine, ComponentType::WebhookEndpoint).unwrap()
    }

    pub(crate) mod fibo {
        use super::*;
        use crate::activity::activity_worker::test::compile_activity;
        use crate::activity::activity_worker::tests::new_activity_fibo;
        use crate::activity::cancel_registry::CancelRegistry;
        use crate::engines::{EngineConfig, Engines};
        use crate::http_hooks::ConfigSectionHint;
        use crate::http_request_policy::{AllowedHostConfig, HostPattern, MethodsPattern};
        use crate::std_output_stream::StdOutputConfig;
        use crate::testing_fn_registry::TestingFnRegistry;
        use crate::webhook::webhook_trigger::{
            self, WebhookEndpointCompiled, WebhookEndpointConfig, WebhookServerError,
            WebhookServerState,
        };
        use crate::workflow::workflow_worker::JoinNextBlockingStrategy;
        use crate::workflow::workflow_worker::test::compile_workflow;
        use crate::workflow::workflow_worker::tests::{FIBOA_WORKFLOW_FFQN, new_workflow_fibo};
        use concepts::component_id::ComponentDigest;
        use concepts::prefixed_ulid::{DEPLOYMENT_ID_DUMMY, RunId};
        use concepts::storage::DbPoolCloseable;
        use concepts::time::ClockFn;
        use concepts::time::TokioSleep;
        use concepts::{ComponentId, ComponentType, Params, StrVariant};
        use concepts::{ExecutionId, storage::DbPool};
        use db_tests::{Database, DbGuard, DbPoolCloseableWrapper};
        use executor::executor::{ExecTask, LockingStrategy};
        use rstest::rstest;
        use serde_json::json;
        use std::net::SocketAddr;
        use std::str::FromStr;
        use std::sync::Arc;
        use std::time::Duration;
        use test_db_macro::expand_enum_database;
        use test_utils::sim_clock::SimClock;
        use tokio::net::TcpListener;
        use tokio::sync::{mpsc, watch};
        use tracing::info;
        use utils::sha256sum::calculate_sha256_file;

        struct SetUpFiboWebhook {
            #[expect(dead_code)]
            set: tokio::task::JoinSet<Result<(), WebhookServerError>>,
            #[expect(dead_code)]
            guard: DbGuard,
            db_pool: Arc<dyn DbPool>,
            server_addr: SocketAddr,
            activity_exec: ExecTask,
            workflow_exec: ExecTask,
            sim_clock: SimClock,
            db_close: DbPoolCloseableWrapper,
            #[expect(dead_code)]
            server_termination_sender: watch::Sender<()>,
            #[expect(dead_code)]
            wh_server_state_sender: watch::Sender<Arc<WebhookServerState>>,
        }

        impl SetUpFiboWebhook {
            async fn new(
                db: db_tests::Database,
                locking_strategy: LockingStrategy,
            ) -> SetUpFiboWebhook {
                let addr = SocketAddr::from(([127, 0, 0, 1], 0));
                let sim_clock = SimClock::default();
                let (guard, db_pool, db_close) = db.set_up().await;
                let activity_exec = new_activity_fibo(
                    db_pool.clone(),
                    sim_clock.clone_box(),
                    TokioSleep,
                    locking_strategy,
                )
                .await;

                let (workflow_runnable, workflow_component_id) = compile_workflow(
                    test_programs_fibo_workflow_builder::TEST_PROGRAMS_FIBO_WORKFLOW,
                )
                .await;

                let fn_registry = TestingFnRegistry::new_from_components(vec![
                    compile_activity(
                        test_programs_fibo_activity_builder::TEST_PROGRAMS_FIBO_ACTIVITY,
                    )
                    .await,
                    (workflow_runnable, workflow_component_id),
                ]);
                let cancel_registry = CancelRegistry::new();
                let engine =
                    Engines::get_webhook_engine(EngineConfig::on_demand_testing()).unwrap();
                let workflow_exec = new_workflow_fibo(
                    db_pool.clone(),
                    sim_clock.clone_box(),
                    JoinNextBlockingStrategy::Interrupt,
                    &fn_registry,
                    cancel_registry,
                    locking_strategy,
                )
                .await;
                let (db_forwarder_sender, _) = mpsc::channel(1);
                let wasm_file = test_programs_fibo_webhook_builder::TEST_PROGRAMS_FIBO_WEBHOOK;
                let router = {
                    let runnable_component =
                        RunnableComponent::new(wasm_file, &engine, ComponentType::WebhookEndpoint)
                            .unwrap();
                    let instance = WebhookEndpointCompiled::new(
                        WebhookEndpointConfig {
                            component_id: ComponentId::new(
                                ComponentType::WebhookEndpoint,
                                StrVariant::empty(),
                                ComponentDigest(calculate_sha256_file(wasm_file).await.unwrap().0),
                            )
                            .unwrap(),
                            forward_stdout: Some(StdOutputConfig::Stdout),
                            forward_stderr: Some(StdOutputConfig::Stdout),
                            env_vars: Arc::from([]),
                            fuel: None,
                            backtrace_persist: false,
                            subscription_interruption: None,
                            logs_store_min_level: None,
                            allowed_hosts: Arc::from([]),
                            js_config: None,
                            config_section_hint: ConfigSectionHint::WebhookEndpointWasm,
                        },
                        runnable_component,
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
                let (server_termination_sender, server_termination_watcher) = watch::channel(());
                let initial_state = Arc::new(webhook_trigger::WebhookServerState {
                    deployment_id: DEPLOYMENT_ID_DUMMY,
                    router: Arc::new(router),
                    fn_registry,
                });
                let (wh_server_state_sender, wh_server_state_watcher) =
                    watch::channel(initial_state);
                let mut set = tokio::task::JoinSet::new();
                set.spawn(webhook_trigger::server(
                    "test".to_string(),
                    tcp_listener,
                    engine,
                    wh_server_state_watcher,
                    db_forwarder_sender,
                    db_pool.clone(),
                    sim_clock.clone_box(),
                    Arc::new(TokioSleep),
                    None,
                    server_termination_watcher,
                ));
                SetUpFiboWebhook {
                    set,
                    guard,
                    db_pool,
                    server_addr,
                    activity_exec,
                    workflow_exec,
                    sim_clock,
                    db_close,
                    server_termination_sender,
                    wh_server_state_sender,
                }
            }

            async fn fibo_fetch(
                server_addr: &str,
                n: u8,
                iterations: u32,
                expected_status_code: u16,
            ) -> String {
                let resp = reqwest::get(format!("http://{server_addr}/fibo/{n}/{iterations}"))
                    .await
                    .unwrap();
                assert_eq!(resp.status().as_u16(), expected_status_code);
                resp.text().await.unwrap()
            }

            async fn close(self) {
                self.db_close.close().await;
            }
        }

        #[rstest]
        #[tokio::test]
        async fn hardcoded_result_should_work(
            #[values(LockingStrategy::ByFfqns, LockingStrategy::ByComponentDigest)]
            locking_strategy: LockingStrategy,
        ) {
            test_utils::set_up();
            let fibo_webhook_harness =
                SetUpFiboWebhook::new(Database::Memory, locking_strategy).await;
            let server_addr = fibo_webhook_harness.server_addr.to_string();
            assert_eq!(
                "fiboa(1, 0) = hardcoded: 1",
                SetUpFiboWebhook::fibo_fetch(&server_addr, 1, 0, 200).await
            );
        }

        #[expand_enum_database]
        #[rstest]
        #[tokio::test]
        async fn direct_call_should_work(
            db: Database,
            #[values(LockingStrategy::ByFfqns, LockingStrategy::ByComponentDigest)]
            locking_strategy: LockingStrategy,
        ) {
            test_utils::set_up();
            let fibo_webhook_harness = SetUpFiboWebhook::new(db, locking_strategy).await;
            let server_addr = fibo_webhook_harness.server_addr.to_string();
            let fetch_task =
                tokio::spawn(
                    async move { SetUpFiboWebhook::fibo_fetch(&server_addr, 2, 1, 200).await },
                );

            let now = fibo_webhook_harness.sim_clock.now();
            while fibo_webhook_harness
                .workflow_exec
                .tick_test_await(now, RunId::generate())
                .await
                .is_empty()
            {
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            // At this point the workflow was started successfuly.
            assert_eq!(
                1,
                fibo_webhook_harness
                    .activity_exec
                    .tick_test_await(now, RunId::generate())
                    .await
                    .len()
            );
            // finish workflow
            assert_eq!(
                1,
                fibo_webhook_harness
                    .workflow_exec
                    .tick_test_await(now, RunId::generate())
                    .await
                    .len()
            );
            let res = fetch_task.await.unwrap();
            assert_eq!("fiboa(2, 1) = direct call: 1", res);
        }

        #[expand_enum_database]
        #[rstest]
        #[tokio::test]
        async fn scheduling_should_work(
            db: db_tests::Database,
            #[values(LockingStrategy::ByFfqns, LockingStrategy::ByComponentDigest)]
            locking_strategy: LockingStrategy,
        ) {
            test_utils::set_up();
            let fibo_webhook_harness = SetUpFiboWebhook::new(db, locking_strategy).await;
            let server_addr = fibo_webhook_harness.server_addr.to_string();
            let n = 10;
            let iterations = 1;
            let resp = SetUpFiboWebhook::fibo_fetch(&server_addr, n, iterations, 200).await;

            let execution_id = resp
                .strip_prefix(&format!("fiboa({n}, {iterations}) = scheduled: "))
                .unwrap();
            let execution_id = ExecutionId::from_str(execution_id).unwrap();
            let conn = fibo_webhook_harness.db_pool.connection().await.unwrap();
            let create_req = conn.get_create_request(&execution_id).await.unwrap();
            assert_eq!(FIBOA_WORKFLOW_FFQN, create_req.ffqn);
            let expected_params = Params::from_json_values_test(vec![json!(10), json!(1)]);
            assert_eq!(
                serde_json::to_string(&expected_params).unwrap(),
                serde_json::to_string(&create_req.params).unwrap()
            );
        }

        #[rstest]
        #[tokio::test]
        async fn test_routing_error_handling(
            #[values(LockingStrategy::ByFfqns, LockingStrategy::ByComponentDigest)]
            locking_strategy: LockingStrategy,
        ) {
            test_utils::set_up();
            let fibo_webhook_harness =
                SetUpFiboWebhook::new(Database::Memory, locking_strategy).await;
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

        #[expand_enum_database]
        #[rstest]
        #[tokio::test]
        async fn http_client_traces_should_be_captured(
            db: db_tests::Database,
            #[values(LockingStrategy::ByFfqns, LockingStrategy::ByComponentDigest)]
            locking_strategy: LockingStrategy,
        ) {
            use concepts::storage::ExecutionRequest;
            use concepts::storage::http_client_trace::{
                HttpClientTrace, RequestTrace, ResponseTrace,
            };
            use wiremock::{
                Mock, MockServer, ResponseTemplate,
                matchers::{method, path},
            };
            const BODY: &str = "webhook-http-trace-test-body";
            test_utils::set_up();
            let sim_clock = SimClock::default();
            let (db_guard, db_pool, db_close) = db.set_up().await;

            // Set up mock HTTP server
            let mock_listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
            let mock_address = mock_listener.local_addr().unwrap();
            let mock_allowed_host = format!("http://127.0.0.1:{port}", port = mock_address.port());
            let mock_uri = format!("{mock_allowed_host}/");
            let mock_server = MockServer::builder().listener(mock_listener).start().await;
            Mock::given(method("GET"))
                .and(path("/"))
                .respond_with(ResponseTemplate::new(200).set_body_string(BODY))
                .expect(1)
                .mount(&mock_server)
                .await;

            // Set up fibo workflow/activity workers (needed for schedule call)
            let activity_exec = crate::activity::activity_worker::tests::new_activity_fibo(
                db_pool.clone(),
                sim_clock.clone_box(),
                TokioSleep,
                locking_strategy,
            )
            .await;

            let (workflow_runnable, workflow_component_id) =
                compile_workflow(test_programs_fibo_workflow_builder::TEST_PROGRAMS_FIBO_WORKFLOW)
                    .await;

            let fn_registry =
                crate::testing_fn_registry::TestingFnRegistry::new_from_components(vec![
                    crate::activity::activity_worker::test::compile_activity(
                        test_programs_fibo_activity_builder::TEST_PROGRAMS_FIBO_ACTIVITY,
                    )
                    .await,
                    (workflow_runnable, workflow_component_id),
                ]);

            let engine = crate::engines::Engines::get_webhook_engine(
                crate::engines::EngineConfig::on_demand_testing(),
            )
            .unwrap();

            // Build the HTTP GET webhook
            let (db_forwarder_sender, _) = mpsc::channel(1);
            let wasm_file = test_programs_http_get_webhook_builder::TEST_PROGRAMS_HTTP_GET_WEBHOOK;
            let router = {
                let runnable_component =
                    RunnableComponent::new(wasm_file, &engine, ComponentType::WebhookEndpoint)
                        .unwrap();
                let instance = WebhookEndpointCompiled::new(
                    WebhookEndpointConfig {
                        component_id: ComponentId::new(
                            ComponentType::WebhookEndpoint,
                            StrVariant::empty(),
                            concepts::component_id::ComponentDigest(
                                utils::sha256sum::calculate_sha256_file(wasm_file)
                                    .await
                                    .unwrap()
                                    .0,
                            ),
                        )
                        .unwrap(),
                        forward_stdout: Some(StdOutputConfig::Stdout),
                        forward_stderr: Some(StdOutputConfig::Stdout),
                        env_vars: Arc::from([]),
                        fuel: None,
                        backtrace_persist: false,
                        subscription_interruption: None,
                        logs_store_min_level: None,
                        allowed_hosts: Arc::from(vec![AllowedHostConfig {
                            pattern: HostPattern::parse_with_methods(
                                &mock_allowed_host,
                                MethodsPattern::AllMethods,
                            )
                            .unwrap(),
                            secret_env_mappings: Vec::new(),
                            replace_in: hashbrown::HashSet::new(),
                        }]),
                        js_config: None,
                        config_section_hint: ConfigSectionHint::WebhookEndpointWasm,
                    },
                    runnable_component,
                )
                .unwrap()
                .link(&engine, fn_registry.as_ref())
                .unwrap();
                let mut router = MethodAwareRouter::default();
                router.add(Some(Method::GET), "/http-get/:PORT", instance);
                router
            };

            let tcp_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let server_addr = tcp_listener.local_addr().unwrap();
            info!("Listening on port {}", server_addr.port());
            let (_server_termination_sender, server_termination_watcher) = watch::channel(());
            let (_wh_server_state_sender, wh_server_state_watcher) =
                watch::channel(Arc::new(WebhookServerState {
                    deployment_id: DEPLOYMENT_ID_DUMMY,
                    router: Arc::new(router),
                    fn_registry,
                }));
            let mut set = tokio::task::JoinSet::new();
            set.spawn(webhook_trigger::server(
                "test".to_string(),
                tcp_listener,
                engine,
                wh_server_state_watcher,
                db_forwarder_sender,
                db_pool.clone(),
                sim_clock.clone_box(),
                Arc::new(TokioSleep),
                None,
                server_termination_watcher,
            ));

            // Send request to webhook with mock server port as route param
            let mock_port = mock_address.port();
            let resp = reqwest::get(format!("http://{server_addr}/http-get/{mock_port}"))
                .await
                .unwrap();
            assert_eq!(resp.status().as_u16(), 200);
            let resp_body = resp.text().await.unwrap();
            let mut lines = resp_body.lines();
            assert_eq!(Some(BODY), lines.next());
            let child_execution_id_str = lines.next().expect("response must contain execution id");
            let child_execution_id = ExecutionId::from_str(child_execution_id_str).unwrap();

            // Find the webhook execution via the child's scheduled_by
            let conn = db_pool.connection().await.unwrap();
            let create_req = conn.get_create_request(&child_execution_id).await.unwrap();
            let webhook_exec_id = create_req
                .scheduled_by
                .expect("child must have scheduled_by set");

            // Poll until the Finished event is persisted
            let finished_event = loop {
                let exec_log = conn.get(&webhook_exec_id).await.unwrap();
                if let ExecutionRequest::Finished {
                    http_client_traces, ..
                } = &exec_log.last_event().event
                {
                    break http_client_traces.clone();
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            };

            let http_client_traces = finished_event
                .as_ref()
                .expect("http_client_traces must be Some");
            assert_eq!(1, http_client_traces.len());
            let trace = &http_client_traces[0];
            assert_matches!(
                trace,
                HttpClientTrace {
                    req: RequestTrace {
                        method,
                        uri,
                        sent_at: _,
                    },
                    resp: Some(ResponseTrace {
                        status: Ok(200),
                        finished_at: _,
                    }),
                } => {
                    assert_eq!("GET", method);
                    assert_eq!(&mock_uri, uri);
                }
            );

            drop(conn);
            drop(activity_exec);
            drop(set);
            drop(db_guard);
            db_close.close().await;
        }

        #[tokio::test]
        async fn http_get_denied_host() {
            use wiremock::{
                Mock, MockServer, ResponseTemplate,
                matchers::{method, path},
            };
            test_utils::set_up();
            let sim_clock = SimClock::default();
            let (db_guard, db_pool, db_close) = Database::Memory.set_up().await;

            // Set up mock HTTP server that the webhook will try to call
            let mock_listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
            let mock_address = mock_listener.local_addr().unwrap();
            let mock_server = MockServer::builder().listener(mock_listener).start().await;
            Mock::given(method("GET"))
                .and(path("/"))
                .respond_with(ResponseTemplate::new(200).set_body_string("should-not-reach"))
                .expect(0) // Should NOT be called since host is not allowed
                .mount(&mock_server)
                .await;

            // Set up activity/workflow workers
            let activity_exec = crate::activity::activity_worker::tests::new_activity_fibo(
                db_pool.clone(),
                sim_clock.clone_box(),
                TokioSleep,
                LockingStrategy::ByComponentDigest,
            )
            .await;

            let (workflow_runnable, workflow_component_id) =
                compile_workflow(test_programs_fibo_workflow_builder::TEST_PROGRAMS_FIBO_WORKFLOW)
                    .await;

            let fn_registry =
                crate::testing_fn_registry::TestingFnRegistry::new_from_components(vec![
                    crate::activity::activity_worker::test::compile_activity(
                        test_programs_fibo_activity_builder::TEST_PROGRAMS_FIBO_ACTIVITY,
                    )
                    .await,
                    (workflow_runnable, workflow_component_id),
                ]);

            let engine = crate::engines::Engines::get_webhook_engine(
                crate::engines::EngineConfig::on_demand_testing(),
            )
            .unwrap();

            // Build the HTTP GET webhook with NO allowed hosts
            let (db_forwarder_sender, _) = mpsc::channel(1);
            let wasm_file = test_programs_http_get_webhook_builder::TEST_PROGRAMS_HTTP_GET_WEBHOOK;
            let router = {
                let runnable_component =
                    RunnableComponent::new(wasm_file, &engine, ComponentType::WebhookEndpoint)
                        .unwrap();
                let instance = WebhookEndpointCompiled::new(
                    WebhookEndpointConfig {
                        component_id: ComponentId::new(
                            ComponentType::WebhookEndpoint,
                            StrVariant::empty(),
                            concepts::component_id::ComponentDigest(
                                utils::sha256sum::calculate_sha256_file(wasm_file)
                                    .await
                                    .unwrap()
                                    .0,
                            ),
                        )
                        .unwrap(),
                        forward_stdout: Some(StdOutputConfig::Stdout),
                        forward_stderr: Some(StdOutputConfig::Stdout),
                        env_vars: Arc::from([]),
                        fuel: None,
                        backtrace_persist: false,
                        subscription_interruption: None,
                        logs_store_min_level: None,
                        allowed_hosts: Arc::from([]), // NO allowed hosts - request should be denied
                        js_config: None,
                        config_section_hint: ConfigSectionHint::WebhookEndpointWasm,
                    },
                    runnable_component,
                )
                .unwrap()
                .link(&engine, fn_registry.as_ref())
                .unwrap();
                let mut router = MethodAwareRouter::default();
                router.add(Some(Method::GET), "/http-get/:PORT", instance);
                router
            };

            let tcp_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let server_addr = tcp_listener.local_addr().unwrap();
            info!("Listening on port {}", server_addr.port());
            let (_server_termination_sender, server_termination_watcher) = watch::channel(());
            let (_wh_server_state_sender, wh_server_state_watcher) =
                watch::channel(Arc::new(WebhookServerState {
                    deployment_id: DEPLOYMENT_ID_DUMMY,
                    router: Arc::new(router),
                    fn_registry,
                }));
            let mut set = tokio::task::JoinSet::new();
            set.spawn(webhook_trigger::server(
                "test".to_string(),
                tcp_listener,
                engine,
                wh_server_state_watcher,
                db_forwarder_sender,
                db_pool.clone(),
                sim_clock.clone_box(),
                Arc::new(TokioSleep),
                None,
                server_termination_watcher,
            ));

            // Send request to webhook
            let mock_port = mock_address.port();
            let resp = reqwest::get(format!("http://{server_addr}/http-get/{mock_port}"))
                .await
                .unwrap();
            // The webhook should return 500 because the HTTP request was denied
            assert_eq!(resp.status().as_u16(), 500);
            let body = resp.text().await.unwrap();
            assert_eq!("Component Error", body);

            // Verify mock was not called
            mock_server.verify().await;

            drop(activity_exec);
            drop(set);
            drop(db_guard);
            db_close.close().await;
        }
    }

    pub(crate) mod js_runtime {
        use crate::RunnableComponent;
        use crate::engines::{EngineConfig, Engines};
        use crate::std_output_stream::StdOutputConfig;
        use crate::testing_fn_registry::TestingFnRegistry;
        use crate::webhook::webhook_trigger::{
            self, MethodAwareRouter, WebhookEndpointCompiled, WebhookEndpointConfig,
            WebhookEndpointJsConfig, WebhookServerError, WebhookServerState,
        };
        use concepts::component_id::ComponentDigest;
        use concepts::prefixed_ulid::DEPLOYMENT_ID_DUMMY;
        use concepts::time::{ClockFn, TokioSleep};
        use concepts::{ComponentId, ComponentType, StrVariant};
        use std::net::SocketAddr;
        use std::sync::Arc;
        use test_utils::sim_clock::SimClock;
        use tokio::net::TcpListener;
        use tokio::sync::{mpsc, watch};
        use tracing::info;
        use utils::sha256sum::calculate_sha256_file;

        struct WatchGuard {
            #[expect(dead_code)]
            server_termination_sender: watch::Sender<()>,
            #[expect(dead_code)]
            wh_server_state_sender: watch::Sender<Arc<WebhookServerState>>,
        }

        async fn start_js_webhook_server(
            source: &str,
        ) -> (
            tokio::task::JoinSet<Result<(), WebhookServerError>>,
            SocketAddr,
            WatchGuard,
        ) {
            let sim_clock = SimClock::default();
            let (_guard, db_pool, _db_close) = db_tests::Database::Memory.set_up().await;
            let fn_registry = TestingFnRegistry::new_from_components(vec![]);
            let engine = Engines::get_webhook_engine(EngineConfig::on_demand_testing()).unwrap();
            let (db_forwarder_sender, _) = mpsc::channel(1);
            let wasm_file = webhook_js_runtime_builder::WEBHOOK_JS_RUNTIME;
            let router = {
                let runnable_component =
                    RunnableComponent::new(wasm_file, &engine, ComponentType::WebhookEndpoint)
                        .unwrap();
                let instance = WebhookEndpointCompiled::new(
                    WebhookEndpointConfig {
                        component_id: ComponentId::new(
                            ComponentType::WebhookEndpoint,
                            StrVariant::empty(),
                            ComponentDigest(calculate_sha256_file(wasm_file).await.unwrap().0),
                        )
                        .unwrap(),
                        forward_stdout: Some(StdOutputConfig::Stdout),
                        forward_stderr: Some(StdOutputConfig::Stdout),
                        env_vars: Arc::from([]),
                        fuel: None,
                        backtrace_persist: false,
                        subscription_interruption: None,
                        logs_store_min_level: None,
                        allowed_hosts: Arc::from([]),
                        js_config: Some(WebhookEndpointJsConfig {
                            source: source.to_string(),
                            file_name: String::new(),
                        }),
                        config_section_hint:
                            crate::http_hooks::ConfigSectionHint::WebhookEndpointJs,
                    },
                    runnable_component,
                )
                .unwrap()
                .link(&engine, fn_registry.as_ref())
                .unwrap();
                let mut router = MethodAwareRouter::default();
                router.add(None, "", instance);
                router
            };
            let tcp_listener = TcpListener::bind(SocketAddr::from(([127, 0, 0, 1], 0)))
                .await
                .unwrap();
            let server_addr = tcp_listener.local_addr().unwrap();
            info!("JS webhook listening on port {}", server_addr.port());
            let (server_termination_sender, server_termination_watcher) = watch::channel(());
            let (wh_server_state_sender, wh_server_state_watcher) =
                watch::channel(Arc::new(WebhookServerState {
                    deployment_id: DEPLOYMENT_ID_DUMMY,
                    router: Arc::new(router),
                    fn_registry,
                }));
            let mut set = tokio::task::JoinSet::new();
            set.spawn(webhook_trigger::server(
                "test-js".to_string(),
                tcp_listener,
                engine,
                wh_server_state_watcher,
                db_forwarder_sender,
                db_pool,
                sim_clock.clone_box(),
                Arc::new(TokioSleep),
                None,
                server_termination_watcher,
            ));
            (
                set,
                server_addr,
                WatchGuard {
                    server_termination_sender,
                    wh_server_state_sender,
                },
            )
        }

        #[tokio::test]
        async fn webhook_js_hello_world() {
            test_utils::set_up();
            let js_source = r#"
                export default function handle(request) {
                    return new Response("Hello from JS!", {
                        status: 200,
                        headers: { "content-type": "text/plain" },
                    });
                }
            "#;
            let (_server, server_addr, _termination_sender) =
                start_js_webhook_server(js_source).await;
            let resp = reqwest::get(format!("http://{server_addr}/"))
                .await
                .unwrap();
            assert_eq!(resp.status().as_u16(), 200);
            assert_eq!("Hello from JS!", resp.text().await.unwrap());
        }

        #[tokio::test]
        async fn webhook_js_reads_request_method_and_url() {
            test_utils::set_up();
            let js_source = r#"
                export default function handle(request) {
                    return new Response(request.method + " " + request.url);
                }
            "#;
            let (_server, server_addr, _termination_sender) =
                start_js_webhook_server(js_source).await;
            let resp = reqwest::get(format!("http://{server_addr}/some/path"))
                .await
                .unwrap();
            assert_eq!(resp.status().as_u16(), 200);
            let body = resp.text().await.unwrap();
            assert!(
                body.contains("GET") && body.contains("/some/path"),
                "Expected method and path in response, got: {body}"
            );
        }

        #[tokio::test]
        async fn webhook_js_custom_status_code() {
            test_utils::set_up();
            let js_source = r#"
                export default function handle(request) {
                    return new Response("created", { status: 201 });
                }
            "#;
            let (_server, server_addr, _termination_sender) =
                start_js_webhook_server(js_source).await;
            let resp = reqwest::get(format!("http://{server_addr}/"))
                .await
                .unwrap();
            assert_eq!(resp.status().as_u16(), 201);
            assert_eq!("created", resp.text().await.unwrap());
        }

        async fn start_js_webhook_server_with_http(
            source: &str,
            allowed_host: &str,
        ) -> (
            tokio::task::JoinSet<Result<(), WebhookServerError>>,
            SocketAddr,
            WatchGuard,
        ) {
            use crate::http_request_policy::{AllowedHostConfig, HostPattern, MethodsPattern};
            let host_pattern =
                HostPattern::parse_with_methods(allowed_host, MethodsPattern::AllMethods).unwrap();
            let sim_clock = SimClock::default();
            let (_guard, db_pool, _db_close) = db_tests::Database::Memory.set_up().await;
            let fn_registry = TestingFnRegistry::new_from_components(vec![]);
            let engine = Engines::get_webhook_engine(EngineConfig::on_demand_testing()).unwrap();
            let (db_forwarder_sender, _) = mpsc::channel(1);
            let wasm_file = webhook_js_runtime_builder::WEBHOOK_JS_RUNTIME;
            let router = {
                let runnable_component =
                    RunnableComponent::new(wasm_file, &engine, ComponentType::WebhookEndpoint)
                        .unwrap();
                let instance = WebhookEndpointCompiled::new(
                    WebhookEndpointConfig {
                        component_id: ComponentId::new(
                            ComponentType::WebhookEndpoint,
                            StrVariant::empty(),
                            ComponentDigest(calculate_sha256_file(wasm_file).await.unwrap().0),
                        )
                        .unwrap(),
                        forward_stdout: Some(StdOutputConfig::Stdout),
                        forward_stderr: Some(StdOutputConfig::Stdout),
                        env_vars: Arc::from([]),
                        fuel: None,
                        backtrace_persist: false,
                        subscription_interruption: None,
                        logs_store_min_level: None,
                        allowed_hosts: Arc::from(vec![AllowedHostConfig {
                            pattern: host_pattern,
                            secret_env_mappings: Vec::new(),
                            replace_in: hashbrown::HashSet::new(),
                        }]),
                        js_config: Some(WebhookEndpointJsConfig {
                            source: source.to_string(),
                            file_name: String::new(),
                        }),
                        config_section_hint:
                            crate::http_hooks::ConfigSectionHint::WebhookEndpointJs,
                    },
                    runnable_component,
                )
                .unwrap()
                .link(&engine, fn_registry.as_ref())
                .unwrap();
                let mut router = MethodAwareRouter::default();
                router.add(None, "", instance);
                router
            };
            let tcp_listener = TcpListener::bind(SocketAddr::from(([127, 0, 0, 1], 0)))
                .await
                .unwrap();
            let server_addr = tcp_listener.local_addr().unwrap();
            info!(
                "JS webhook with HTTP listening on port {}",
                server_addr.port()
            );
            let (server_termination_sender, server_termination_watcher) = watch::channel(());
            let (wh_server_state_sender, wh_server_state_watcher) =
                watch::channel(Arc::new(WebhookServerState {
                    deployment_id: DEPLOYMENT_ID_DUMMY,
                    router: Arc::new(router),
                    fn_registry,
                }));
            let mut set = tokio::task::JoinSet::new();
            set.spawn(webhook_trigger::server(
                "test-js-http".to_string(),
                tcp_listener,
                engine,
                wh_server_state_watcher,
                db_forwarder_sender,
                db_pool,
                sim_clock.clone_box(),
                Arc::new(TokioSleep),
                None,
                server_termination_watcher,
            ));
            (
                set,
                server_addr,
                WatchGuard {
                    server_termination_sender,
                    wh_server_state_sender,
                },
            )
        }

        #[tokio::test]
        async fn webhook_js_fetch_get() {
            use wiremock::{
                Mock, MockServer, ResponseTemplate,
                matchers::{method, path},
            };
            test_utils::set_up();
            let mock_server = MockServer::start().await;
            Mock::given(method("GET"))
                .and(path("/hello"))
                .respond_with(ResponseTemplate::new(200).set_body_string("fetch works"))
                .expect(1)
                .mount(&mock_server)
                .await;

            let url = mock_server.uri();
            let js_source = format!(
                r#"
                export default async function handle(request) {{
                    const resp = await fetch("{url}/hello");
                    const text = await resp.text();
                    return new Response(text);
                }}
                "#
            );

            let allowed = format!("http://127.0.0.1:{}", mock_server.address().port());
            let (_server, server_addr, _termination_sender) =
                start_js_webhook_server_with_http(&js_source, &allowed).await;
            let resp = reqwest::get(format!("http://{server_addr}/"))
                .await
                .unwrap();
            let status = resp.status().as_u16();
            let body = resp.text().await.unwrap();
            assert_eq!((status, body.as_str()), (200, "fetch works"));
        }

        #[tokio::test]
        async fn webhook_js_request_headers() {
            test_utils::set_up();
            // JS handler that returns the x-custom header values as a JSON array.
            // request.headers is a proper Headers object; multiple values for the
            // same header name are combined with a comma by Headers.get().
            let js_source = r#"
                export default function handle(request) {
                    const value = request.headers.get("x-custom");
                    console.log("header value:`" +value + "`");
                    return Response.json(value !== null ? value.split(", ") : []);
                }
                "#;

            let (_server, server_addr, _termination_sender) =
                start_js_webhook_server(js_source).await;

            // Send request with multiple values for the same header
            let client = reqwest::Client::new();
            let resp = client
                .get(format!("http://{server_addr}/"))
                .header("x-custom", "value1")
                .header("x-custom", "value2")
                .send()
                .await
                .unwrap();

            let status = resp.status().as_u16();
            let body = resp.text().await.unwrap();
            assert_eq!(status, 200);
            let headers: Vec<String> = serde_json::from_str(&body).unwrap();
            assert_eq!(headers, vec!["value1", "value2"]);
        }

        #[tokio::test]
        async fn webhook_js_fetch_proxy_headers() {
            use wiremock::{
                Mock, MockServer, ResponseTemplate,
                matchers::{header, method},
            };
            test_utils::set_up();
            let mock_server = MockServer::start().await;
            Mock::given(method("GET"))
                .and(header("x-forwarded", "proxy-value"))
                .respond_with(ResponseTemplate::new(200).set_body_string("proxied"))
                .expect(1)
                .mount(&mock_server)
                .await;

            let url = mock_server.uri();
            let js_source = format!(
                r#"
                export default async function handle(request) {{
                    console.log("Got headers", [...request.headers]);
                    return fetch("{url}/", {{ headers: request.headers }});
                }}
                "#
            );

            let allowed = format!("http://127.0.0.1:{}", mock_server.address().port());
            let (_server, server_addr, _termination_sender) =
                start_js_webhook_server_with_http(&js_source, &allowed).await;

            let client = reqwest::Client::new();
            let resp = client
                .get(format!("http://{server_addr}/"))
                .header("x-forwarded", "proxy-value")
                .send()
                .await
                .unwrap();

            let status = resp.status().as_u16();
            let body = resp.text().await.unwrap();
            assert_eq!((status, body.as_str()), (200, "proxied"));
        }

        #[tokio::test]
        async fn webhook_js_env_var() {
            test_utils::set_up();
            let js_source = r#"
                export default function handle(request) {
                    const jsSource = process.env["__OBELISK_JS_SOURCE__"];
                    const missing = process.env["MISSING_VAR"];
                    return Response.json({
                        hasJsSource: jsSource !== undefined,
                        missingIsUndefined: missing === undefined,
                    });
                }
            "#;
            let (_server, server_addr, _termination_sender) =
                start_js_webhook_server(js_source).await;
            let resp = reqwest::get(format!("http://{server_addr}/"))
                .await
                .unwrap();
            assert_eq!(resp.status().as_u16(), 200);
            let body: serde_json::Value = resp.json().await.unwrap();
            assert_eq!(body["hasJsSource"], serde_json::json!(true));
            assert_eq!(body["missingIsUndefined"], serde_json::json!(true));
        }

        #[tokio::test]
        async fn webhook_js_request_body_text() {
            test_utils::set_up();
            let js_source = r#"
                export default async function handle(request) {
                    const text = await request.text();
                    return new Response(text, { headers: { "content-type": "text/plain" } });
                }
            "#;
            let (_server, server_addr, _termination_sender) =
                start_js_webhook_server(js_source).await;
            let client = reqwest::Client::new();
            let resp = client
                .post(format!("http://{server_addr}/"))
                .body("hello from body")
                .send()
                .await
                .unwrap();
            assert_eq!(resp.status().as_u16(), 200);
            assert_eq!(resp.text().await.unwrap(), "hello from body");
        }

        #[tokio::test]
        async fn webhook_js_request_body_json() {
            test_utils::set_up();
            let js_source = r"
                export default async function handle(request) {
                    const data = await request.json();
                    return Response.json({ received: data });
                }
            ";
            let (_server, server_addr, _termination_sender) =
                start_js_webhook_server(js_source).await;
            let client = reqwest::Client::new();
            let resp = client
                .post(format!("http://{server_addr}/"))
                .header("content-type", "application/json")
                .body(r#"{"name":"world","value":42}"#)
                .send()
                .await
                .unwrap();
            assert_eq!(resp.status().as_u16(), 200);
            let body: serde_json::Value = resp.json().await.unwrap();
            assert_eq!(body["received"]["name"], "world");
            assert_eq!(body["received"]["value"], 42);
        }

        #[tokio::test]
        async fn webhook_js_request_body_form_data() {
            test_utils::set_up();
            let js_source = r"
                export default async function handle(request) {
                    const form = await request.formData();
                    return Response.json(form);
                }
            ";
            let (_server, server_addr, _termination_sender) =
                start_js_webhook_server(js_source).await;
            let client = reqwest::Client::new();
            let resp = client
                .post(format!("http://{server_addr}/"))
                .header("content-type", "application/x-www-form-urlencoded")
                .body("name=Alice&city=Wonderland")
                .send()
                .await
                .unwrap();
            assert_eq!(resp.status().as_u16(), 200);
            let body: serde_json::Value = resp.json().await.unwrap();
            assert_eq!(body["name"], "Alice");
            assert_eq!(body["city"], "Wonderland");
        }

        #[tokio::test]
        async fn webhook_js_request_body_empty() {
            test_utils::set_up();
            // GET request with no body — text() should return an empty string.
            let js_source = r"
                export default async function handle(request) {
                    const text = await request.text();
                    return new Response(JSON.stringify({ len: text.length }));
                }
            ";
            let (_server, server_addr, _termination_sender) =
                start_js_webhook_server(js_source).await;
            let resp = reqwest::get(format!("http://{server_addr}/"))
                .await
                .unwrap();
            assert_eq!(resp.status().as_u16(), 200);
            let body: serde_json::Value = resp.json().await.unwrap();
            assert_eq!(body["len"], 0);
        }

        #[tokio::test]
        async fn webhook_js_generate_execution_id() {
            test_utils::set_up();
            let js_source = r#"
                export default function handle(request) {
                    const id1 = obelisk.executionIdGenerate();
                    const id2 = obelisk.executionIdGenerate();
                    return Response.json({
                        id1,
                        id2,
                        different: id1 !== id2,
                        hasPrefix: id1.startsWith("E_"),
                    });
                }
            "#;
            let (_server, server_addr, _termination_sender) =
                start_js_webhook_server(js_source).await;
            let resp = reqwest::get(format!("http://{server_addr}/"))
                .await
                .unwrap();
            assert_eq!(resp.status().as_u16(), 200);
            let body: serde_json::Value = resp.json().await.unwrap();
            assert_eq!(body["different"], serde_json::json!(true));
            assert_eq!(body["hasPrefix"], serde_json::json!(true));
        }

        /// Test harness for JS webhook tests that need to call activities/workflows.
        struct JsWebhookWithActivitiesHarness {
            #[expect(dead_code)]
            server_set: tokio::task::JoinSet<Result<(), WebhookServerError>>,
            server_addr: SocketAddr,
            activity_exec: executor::executor::ExecTask,
            sim_clock: SimClock,
            db_pool: Arc<dyn concepts::storage::DbPool>,
            #[expect(dead_code)]
            db_close: db_tests::DbPoolCloseableWrapper,
            #[expect(dead_code)]
            guard: WatchGuard,
        }

        impl JsWebhookWithActivitiesHarness {
            async fn new(js_source: &str) -> Self {
                use crate::activity::activity_worker::test::compile_activity;
                use crate::activity::activity_worker::tests::new_activity_fibo;
                use concepts::time::TokioSleep;
                use executor::executor::LockingStrategy;

                let sim_clock = SimClock::default();
                let (_guard, db_pool, db_close) = db_tests::Database::Memory.set_up().await;

                // Set up fibo activity worker
                let activity_exec = new_activity_fibo(
                    db_pool.clone(),
                    sim_clock.clone_box(),
                    TokioSleep,
                    LockingStrategy::ByComponentDigest,
                )
                .await;

                // Create fn_registry with fibo activity
                let fn_registry = TestingFnRegistry::new_from_components(vec![
                    compile_activity(
                        test_programs_fibo_activity_builder::TEST_PROGRAMS_FIBO_ACTIVITY,
                    )
                    .await,
                ]);

                let engine =
                    Engines::get_webhook_engine(EngineConfig::on_demand_testing()).unwrap();
                let (db_forwarder_sender, _) = mpsc::channel(1);
                let wasm_file = webhook_js_runtime_builder::WEBHOOK_JS_RUNTIME;

                let router = {
                    let runnable_component =
                        RunnableComponent::new(wasm_file, &engine, ComponentType::WebhookEndpoint)
                            .unwrap();
                    let instance = WebhookEndpointCompiled::new(
                        WebhookEndpointConfig {
                            component_id: ComponentId::new(
                                ComponentType::WebhookEndpoint,
                                StrVariant::empty(),
                                ComponentDigest(calculate_sha256_file(wasm_file).await.unwrap().0),
                            )
                            .unwrap(),
                            forward_stdout: Some(StdOutputConfig::Stdout),
                            forward_stderr: Some(StdOutputConfig::Stdout),
                            env_vars: Arc::from([]),
                            fuel: None,
                            backtrace_persist: false,
                            subscription_interruption: None,
                            logs_store_min_level: None,
                            allowed_hosts: Arc::from([]),
                            js_config: Some(WebhookEndpointJsConfig {
                                source: js_source.to_string(),
                                file_name: String::new(),
                            }),
                            config_section_hint:
                                crate::http_hooks::ConfigSectionHint::WebhookEndpointJs,
                        },
                        runnable_component,
                    )
                    .unwrap()
                    .link(&engine, fn_registry.as_ref())
                    .unwrap();
                    let mut router = MethodAwareRouter::default();
                    router.add(None, "", instance);
                    router
                };

                let tcp_listener = TcpListener::bind(SocketAddr::from(([127, 0, 0, 1], 0)))
                    .await
                    .unwrap();
                let server_addr = tcp_listener.local_addr().unwrap();
                info!(
                    "JS webhook with activities listening on port {}",
                    server_addr.port()
                );
                let (server_termination_sender, server_termination_watcher) = watch::channel(());
                let (wh_server_state_sender, wh_server_state_watcher) =
                    watch::channel(Arc::new(WebhookServerState {
                        deployment_id: DEPLOYMENT_ID_DUMMY,
                        router: Arc::new(router),
                        fn_registry,
                    }));
                let mut server_set = tokio::task::JoinSet::new();
                server_set.spawn(webhook_trigger::server(
                    "test-js-activities".to_string(),
                    tcp_listener,
                    engine,
                    wh_server_state_watcher,
                    db_forwarder_sender,
                    db_pool.clone(),
                    sim_clock.clone_box(),
                    Arc::new(TokioSleep),
                    None,
                    server_termination_watcher,
                ));

                Self {
                    server_set,
                    server_addr,
                    activity_exec,
                    sim_clock,
                    db_pool,
                    db_close,
                    guard: WatchGuard {
                        server_termination_sender,
                        wh_server_state_sender,
                    },
                }
            }

            async fn tick_activity(&self) -> usize {
                use concepts::prefixed_ulid::RunId;
                self.activity_exec
                    .tick_test_await(self.sim_clock.now(), RunId::generate())
                    .await
                    .len()
            }
        }

        #[tokio::test]
        async fn webhook_js_call_activity() {
            test_utils::set_up();
            let js_source = r#"
                export default function handle(request) {
                    // Call fibo(10) directly
                    const result = obelisk.call("testing:fibo/fibo.fibo", [10]);
                    return Response.json({ result });
                }
            "#;

            let harness = JsWebhookWithActivitiesHarness::new(js_source).await;

            // Start the webhook request in background
            let server_addr = harness.server_addr;
            let fetch_task = tokio::spawn(async move {
                reqwest::get(format!("http://{server_addr}/"))
                    .await
                    .unwrap()
            });

            // Poll until the activity is pending and execute it
            while harness.tick_activity().await == 0 {
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }

            // Get the response
            let resp = fetch_task.await.unwrap();
            assert_eq!(resp.status().as_u16(), 200);
            let body: serde_json::Value = resp.json().await.unwrap();
            assert_eq!(body["result"], serde_json::json!(55)); // fibo(10) = 55
        }

        #[tokio::test]
        async fn webhook_js_schedule_activity() {
            use std::str::FromStr as _;

            test_utils::set_up();
            let js_source = r#"
                export default function handle(request) {
                    // Schedule fibo(10) for later execution
                    const execId = obelisk.executionIdGenerate();
                    obelisk.schedule(execId, "testing:fibo/fibo.fibo", [10], { seconds: 60 });
                    return Response.json({ execId });
                }
            "#;

            let harness = JsWebhookWithActivitiesHarness::new(js_source).await;
            let resp = reqwest::get(format!("http://{}/", harness.server_addr))
                .await
                .unwrap();
            assert_eq!(resp.status().as_u16(), 200);
            let body: serde_json::Value = resp.json().await.unwrap();

            // Verify an execution ID was returned
            let exec_id_str = body["execId"].as_str().unwrap();
            assert!(
                exec_id_str.starts_with("E_"),
                "Expected execution ID prefix"
            );

            // Verify the execution was created in the database
            let exec_id = concepts::ExecutionId::from_str(exec_id_str).unwrap();
            let conn = harness.db_pool.connection().await.unwrap();
            let create_req = conn.get_create_request(&exec_id).await.unwrap();
            assert_eq!(
                "testing:fibo/fibo.fibo",
                create_req.ffqn.to_string().as_str()
            );
        }

        #[tokio::test]
        async fn webhook_js_get_status() {
            test_utils::set_up();
            let js_source = r#"
                export default function handle(request) {
                    // Schedule for later, then check status
                    const execId = obelisk.executionIdGenerate();
                    obelisk.schedule(execId, "testing:fibo/fibo.fibo", [10], { seconds: 60 });
                    const status = obelisk.getStatus(execId);
                    return Response.json({ execId, executionStatus: status });
                }
            "#;

            let harness = JsWebhookWithActivitiesHarness::new(js_source).await;
            let resp = reqwest::get(format!("http://{}/", harness.server_addr))
                .await
                .unwrap();
            assert_eq!(resp.status().as_u16(), 200);
            let body: serde_json::Value = resp.json().await.unwrap();

            // Verify status is "pendingAt" (scheduled for later)
            assert_eq!(
                body["executionStatus"]["status"],
                serde_json::json!("pendingAt")
            );
        }

        #[tokio::test]
        async fn webhook_js_try_get_pending() {
            test_utils::set_up();
            let js_source = r#"
                export default function handle(request) {
                    // Schedule now but don't wait for completion
                    const execId = obelisk.executionIdGenerate();
                    obelisk.schedule(execId, "testing:fibo/fibo.fibo", [10]);
                    const result = obelisk.tryGet(execId);
                    return Response.json({ result });
                }
            "#;

            let harness = JsWebhookWithActivitiesHarness::new(js_source).await;
            let resp = reqwest::get(format!("http://{}/", harness.server_addr))
                .await
                .unwrap();
            assert_eq!(resp.status().as_u16(), 200);
            let body: serde_json::Value = resp.json().await.unwrap();

            // Should return pending since activity hasn't run yet
            assert_eq!(body["result"]["pending"], serde_json::json!(true));
        }

        #[tokio::test]
        async fn webhook_js_call_with_error() {
            test_utils::set_up();
            // fibo(50) returns Err(()) in the test activity (n > 40 returns error)
            let js_source = r#"
                export default function handle(request) {
                    try {
                        obelisk.call("testing:fibo/fibo.fibo", [50]);
                        return Response.json({ result: "unexpected success" });
                    } catch (e) {
                        return Response.json({ error: e.message });
                    }
                }
            "#;

            let harness = JsWebhookWithActivitiesHarness::new(js_source).await;

            let server_addr = harness.server_addr;
            let fetch_task = tokio::spawn(async move {
                reqwest::get(format!("http://{server_addr}/"))
                    .await
                    .unwrap()
            });

            // Poll until the activity is pending and execute it
            while harness.tick_activity().await == 0 {
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }

            let resp = fetch_task.await.unwrap();
            assert_eq!(resp.status().as_u16(), 200);
            let body: serde_json::Value = resp.json().await.unwrap();
            // Should have caught the error
            assert!(body["error"].is_string());
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
