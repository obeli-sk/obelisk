#![allow(clippy::needless_pass_by_value)]

use super::webhook_trigger::{WebhookEndpointCtx, WebhookEndpointJsConfig, types};
use crate::component_logger::log_activities::obelisk::log::log::Host as LogHost;
use crate::js_imports::{JsDispatchPolicy, NamedFnImport};
use concepts::IfcFqnName;
use deno_core::{
    ModuleLoadOptions, ModuleLoadReferrer, ModuleLoadResponse, ModuleLoader, ModuleSource,
    ModuleSourceCode, ModuleSpecifier, ModuleType, OpState, ResolutionKind, RuntimeOptions, op2,
    resolve_import, v8,
};
use deno_error::JsErrorBox;
use http_body_util::combinators::UnsyncBoxBody;
use hyper::{HeaderMap, Response, StatusCode, body::Bytes};
use serde::Deserialize;
use serde_json::{Value, json};
use std::{
    cell::{Cell, RefCell},
    collections::{BTreeMap, HashMap},
    rc::Rc,
    sync::Arc,
};
use tokio::sync::{mpsc, oneshot};
use types::obelisk::webhook::webhook_support::Host as WebhookSupportHost;
use wasmtime_wasi_http::p2::body::HyperOutgoingBody;

/// Response whose body the isolate keeps writing after the headers were sent.
pub(super) type StreamedResponse = Response<HyperOutgoingBody>;

pub(super) struct NativeRequest {
    pub method: String,
    pub url: String,
    pub headers: Vec<(String, String)>,
    pub body: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct NativeResponse {
    pub status: u16,
    pub status_text: String,
    pub headers: Vec<(String, String)>,
    pub body: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct NativeResponseHead {
    status: u16,
    status_text: String,
    headers: Vec<(String, String)>,
}

enum BodyEvent {
    Data(Bytes),
    End,
    Error(String),
}

/// Response body fed by the isolate; ends with an error unless JS closed it.
struct ChannelBody {
    events: mpsc::Receiver<BodyEvent>,
    done: bool,
}

impl hyper::body::Body for ChannelBody {
    type Data = Bytes;
    type Error = wasmtime_wasi_http::Error;

    fn poll_frame(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Result<hyper::body::Frame<Bytes>, Self::Error>>> {
        if self.done {
            return std::task::Poll::Ready(None);
        }
        let event = std::task::ready!(self.events.poll_recv(cx));
        std::task::Poll::Ready(match event {
            Some(BodyEvent::Data(data)) => Some(Ok(hyper::body::Frame::data(data))),
            Some(BodyEvent::End) => {
                self.done = true;
                None
            }
            Some(BodyEvent::Error(reason)) => {
                self.done = true;
                Some(Err(wasmtime_wasi_http::Error::InternalError(Some(reason))))
            }
            None => {
                self.done = true;
                Some(Err(wasmtime_wasi_http::Error::InternalError(Some(
                    "webhook ended without finishing the response body".into(),
                ))))
            }
        })
    }

    fn is_end_stream(&self) -> bool {
        self.done
    }
}

pub(super) enum NativeWebhookFailure {
    CannotInstantiate(String),
    Execution(String),
    Timeout,
}

/// Import bindings and dispatch policy of a V8 JS webhook, resolved once at link time.
pub(super) struct NativeV8Imports {
    imports: HashMap<IfcFqnName, Vec<NamedFnImport>>,
    dispatch_policy: Arc<JsDispatchPolicy>,
}

impl NativeV8Imports {
    pub(super) fn new(
        files: &BTreeMap<String, String>,
        imports: HashMap<IfcFqnName, Vec<NamedFnImport>>,
    ) -> Result<Self, String> {
        let dispatch_policy = Arc::new(JsDispatchPolicy::new(
            files,
            &imports,
            "obelisk:webhook-dynamic@1.0.0",
        )?);
        Ok(Self {
            imports,
            dispatch_policy,
        })
    }
}

struct HostState {
    dispatch_policy: Arc<JsDispatchPolicy>,
    ctx: usize,
    handle: tokio::runtime::Handle,
    env: HashMap<String, String>,
    panic: crate::v8_panic::V8PanicState,
    /// Module specifier -> deployment-relative file name, for backtrace frames.
    user_module_paths: HashMap<String, String>,
    /// Taken by the first `op_webhook_respond_stream`.
    stream: Option<oneshot::Sender<StreamedResponse>>,
    body: Option<mpsc::Sender<BodyEvent>>,
    /// Set once headers were sent; the request deadline no longer applies.
    responded: Rc<Cell<bool>>,
}

impl HostState {
    fn ctx(&mut self) -> &mut WebhookEndpointCtx {
        // SAFETY: the context outlives V8, and each borrow ends before the isolate resumes.
        unsafe { &mut *(self.ctx as *mut WebhookEndpointCtx) }
    }

    fn block_on<F>(&mut self, future: F) -> F::Output
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        // Drive the host op on the real multi-thread runtime (real wakers) and block the V8
        // thread on a std primitive. Avoids a foreign `block_on` parker that can drop a tokio
        // wakeup (see the workflow runtime for the sqlite-writer lost-wakeup this prevents).
        let (tx, rx) = std::sync::mpsc::sync_channel(1);
        self.handle.spawn(async move {
            let _ = tx.send(future.await);
        });
        rx.recv()
            .expect("webhook host-op task panicked or was dropped")
    }
}

#[derive(Deserialize)]
struct HostRequest {
    op: String,
    #[serde(default)]
    args: Value,
}

#[op2(fast)]
fn op_webhook_log(
    state: &mut OpState,
    #[string] level: String,
    #[string] message: String,
) -> Result<(), JsErrorBox> {
    let panic = state.borrow::<HostState>().panic.clone();
    panic.catch(|| {
        op_webhook_log_inner(state, level, message);
        Ok(())
    })
}

fn op_webhook_log_inner(state: &mut OpState, level: String, message: String) {
    let host = state.borrow_mut::<HostState>();
    // Capture the ctx as a `usize` so the spawned future is `Send`.
    let ctx = std::ptr::from_mut::<WebhookEndpointCtx>(host.ctx()) as usize;
    let future = async move {
        // SAFETY: the isolate blocks until this borrow ends, and execute owns the context through teardown.
        let ctx = unsafe { &mut *(ctx as *mut WebhookEndpointCtx) };
        match level.as_str() {
            "trace" => ctx.trace(message).await,
            "debug" => ctx.debug(message).await,
            "warn" => ctx.warn(message).await,
            "error" => ctx.error(message).await,
            _ => ctx.info(message).await,
        }
    };
    host.block_on(future);
}

#[op2]
#[string]
fn op_webhook_env(state: &mut OpState, #[string] name: String) -> Option<String> {
    let panic = state.borrow::<HostState>().panic.clone();
    panic
        .catch(|| Ok(state.borrow::<HostState>().env.get(&name).cloned()))
        .unwrap_or_default()
}

#[op2(async(deferred), fast)]
async fn op_webhook_sleep(
    state: Rc<RefCell<OpState>>,
    #[number] milliseconds: u64,
) -> Result<(), JsErrorBox> {
    let panic = state.borrow().borrow::<HostState>().panic.clone();
    panic
        .catch_async(async move {
            tokio::time::sleep(std::time::Duration::from_millis(milliseconds)).await;
            Ok(())
        })
        .await
}

/// Sends the response headers; the body follows through `op_webhook_body_*`.
#[op2]
fn op_webhook_respond_stream(
    state: &mut OpState,
    #[serde] head: NativeResponseHead,
) -> Result<(), JsErrorBox> {
    let panic = state.borrow::<HostState>().panic.clone();
    panic.catch(|| {
        let host = state.borrow_mut::<HostState>();
        let stream = host
            .stream
            .take()
            .ok_or_else(|| JsErrorBox::generic("response was already sent"))?;
        let (sender, events) = mpsc::channel(8);
        let response = hyper_response(
            head,
            UnsyncBoxBody::new(ChannelBody {
                events,
                done: false,
            }),
        )
        .map_err(JsErrorBox::type_error)?;
        host.body = Some(sender);
        host.responded.set(true);
        // The supervisor only drops the receiver after giving up on the request.
        let _ = stream.send(response);
        Ok(())
    })
}

/// Returns false once the client stopped reading.
#[op2(async(deferred), fast)]
async fn op_webhook_body_write(
    state: Rc<RefCell<OpState>>,
    #[buffer(copy)] chunk: Bytes,
) -> Result<bool, JsErrorBox> {
    let (panic, sender) = {
        let state = state.borrow();
        let host = state.borrow::<HostState>();
        (host.panic.clone(), host.body.clone())
    };
    panic
        .catch_async(async move {
            let sender = sender.ok_or_else(|| JsErrorBox::generic("response body is not open"))?;
            Ok(sender.send(BodyEvent::Data(chunk)).await.is_ok())
        })
        .await
}

async fn finish_body(state: Rc<RefCell<OpState>>, event: BodyEvent) -> Result<(), JsErrorBox> {
    let (panic, sender) = {
        let mut state = state.borrow_mut();
        let host = state.borrow_mut::<HostState>();
        (host.panic.clone(), host.body.take())
    };
    panic
        .catch_async(async move {
            if let Some(sender) = sender {
                let _ = sender.send(event).await;
            }
            Ok(())
        })
        .await
}

#[op2(async(deferred), fast)]
async fn op_webhook_body_close(state: Rc<RefCell<OpState>>) -> Result<(), JsErrorBox> {
    finish_body(state, BodyEvent::End).await
}

#[op2(async(deferred), fast)]
async fn op_webhook_body_abort(
    state: Rc<RefCell<OpState>>,
    #[string] reason: String,
) -> Result<(), JsErrorBox> {
    finish_body(state, BodyEvent::Error(reason)).await
}

#[op2]
#[serde]
fn op_webhook_host(
    scope: &mut v8::PinScope,
    state: &mut OpState,
    #[serde] request: HostRequest,
) -> Result<serde_json::Value, JsErrorBox> {
    let panic = state.borrow::<HostState>().panic.clone();
    let host = state.borrow_mut::<HostState>();
    let backtrace = if host.ctx().backtrace_persist {
        crate::v8_backtrace::capture(scope, &host.user_module_paths)
    } else {
        None
    };
    panic.catch(|| op_webhook_host_inner(state, request, backtrace))
}

fn op_webhook_host_inner(
    state: &mut OpState,
    request: HostRequest,
    backtrace: Option<concepts::storage::WasmBacktrace>,
) -> Result<serde_json::Value, JsErrorBox> {
    let host = state.borrow_mut::<HostState>();
    if matches!(request.op.as_str(), "call" | "schedule") {
        host.dispatch_policy
            .check(string_arg(&request.args, "target")?)
            .map_err(JsErrorBox::type_error)?;
    }
    // Capture the ctx as a `usize` so the spawned future is `Send`.
    let ctx = std::ptr::from_mut::<WebhookEndpointCtx>(host.ctx()) as usize;
    let future = async move {
        // SAFETY: the isolate blocks until this borrow ends, and execute owns the context through teardown.
        let ctx = unsafe { &mut *(ctx as *mut WebhookEndpointCtx) };
        dispatch_host(ctx, &request.op, &request.args, backtrace).await
    };
    host.block_on(future)
}

deno_core::extension!(
    obelisk_webhook_v8,
    ops = [
        op_webhook_log,
        op_webhook_env,
        op_webhook_sleep,
        op_webhook_respond_stream,
        op_webhook_body_write,
        op_webhook_body_close,
        op_webhook_body_abort,
        op_webhook_host
    ]
);

#[expect(clippy::too_many_arguments)]
pub(super) async fn execute(
    config: &WebhookEndpointJsConfig,
    imports: &NativeV8Imports,
    request: NativeRequest,
    env: HashMap<String, String>,
    ctx: &mut WebhookEndpointCtx,
    handle: tokio::runtime::Handle,
    isolate_tx: tokio::sync::oneshot::Sender<deno_core::v8::IsolateHandle>,
    stream_tx: oneshot::Sender<StreamedResponse>,
    max_heap_size: Option<usize>,
    request_deadline: tokio::time::Instant,
    mut stream_expired: tokio::sync::watch::Receiver<()>,
) -> Result<Option<NativeResponse>, NativeWebhookFailure> {
    let mut connection_drop_watcher = ctx.connection_drop_watcher.clone();
    let mut server_termination_watcher = ctx.server_termination_watcher.clone();
    let loader = Rc::new(InMemoryModuleLoader::new(
        &config.files,
        &imports.imports,
        imports.dispatch_policy.dynamic,
    ));
    let (mut runtime, heap) = crate::v8_heap::new_runtime(
        RuntimeOptions {
            module_loader: Some(loader.clone()),
            extensions: vec![
                obelisk_webhook_v8::init(),
                crate::v8_crypto::obelisk_crypto_v8::init(),
                crate::v8_fetch::obelisk_fetch_v8::init(),
            ],
            startup_snapshot: Some(crate::v8_snapshot::STARTUP_SNAPSHOT),
            ..Default::default()
        },
        max_heap_size,
    );
    let responded = Rc::new(Cell::new(false));
    let execution = execute_inner(
        &mut runtime,
        &loader,
        &config.entry_path,
        request,
        env,
        ctx,
        handle,
        isolate_tx,
        StreamState {
            sender: stream_tx,
            responded: responded.clone(),
        },
        imports.dispatch_policy.clone(),
    );
    // The deadline bounds the time to headers only.
    let deadline = async {
        tokio::time::sleep_until(request_deadline).await;
        if responded.get() {
            std::future::pending::<()>().await;
        }
    };
    let result = tokio::select! {
        result = execution => result,
        Ok(()) = stream_expired.changed() => Ok(None),
        // A client leaving a streamed response is the normal end of e.g. an SSE stream.
        _ = connection_drop_watcher.changed() => if responded.get() {
            Ok(None)
        } else {
            Err(NativeWebhookFailure::Execution("connection closed".into()))
        },
        _ = server_termination_watcher.changed() => Err(NativeWebhookFailure::Execution("server shutdown requested".into())),
        () = deadline => Err(NativeWebhookFailure::Timeout),
    };
    if heap.exhausted() {
        Err(NativeWebhookFailure::Execution(
            crate::v8_heap::EXHAUSTED.into(),
        ))
    } else {
        result
    }
}

struct StreamState {
    sender: oneshot::Sender<StreamedResponse>,
    responded: Rc<Cell<bool>>,
}

#[expect(clippy::too_many_arguments)]
async fn execute_inner(
    runtime: &mut deno_core::JsRuntime,
    loader: &InMemoryModuleLoader,
    entry_path: &str,
    request: NativeRequest,
    env: HashMap<String, String>,
    ctx: &mut WebhookEndpointCtx,
    handle: tokio::runtime::Handle,
    isolate_tx: tokio::sync::oneshot::Sender<deno_core::v8::IsolateHandle>,
    stream: StreamState,
    dispatch_policy: Arc<JsDispatchPolicy>,
) -> Result<Option<NativeResponse>, NativeWebhookFailure> {
    let responded = stream.responded.clone();
    let isolate = runtime.v8_isolate().thread_safe_handle();
    let _ = isolate_tx.send(isolate.clone());
    let panic = crate::v8_panic::V8PanicState::new(isolate);
    let ctx_ptr = std::ptr::from_mut(ctx) as usize;
    runtime
        .op_state()
        .borrow_mut()
        .put(crate::v8_fetch::FetchHost::new(Box::new(
            move |method, uri, headers, body| {
                // SAFETY: the context outlives V8, and the borrow ends before the isolate resumes.
                let ctx = unsafe { &mut *(ctx_ptr as *mut WebhookEndpointCtx) };
                let pending = ctx
                    .http_hooks
                    .send_native_request_streaming(method, uri, headers, body)?;
                Ok(Box::pin(pending) as crate::v8_fetch::PendingFetch)
            },
        )));
    runtime.op_state().borrow_mut().put(HostState {
        dispatch_policy,
        ctx: ctx_ptr,
        handle,
        env,
        panic: panic.clone(),
        user_module_paths: crate::v8_backtrace::user_module_paths(&loader.paths),
        stream: Some(stream.sender),
        body: None,
        responded: stream.responded,
    });
    runtime.op_state().borrow_mut().put(panic.clone());
    runtime
        .execute_script("obelisk:fetch-bootstrap", crate::v8_fetch::FETCH_BOOTSTRAP)
        .map_err(|err| NativeWebhookFailure::CannotInstantiate(err.to_string()))?;
    runtime
        .execute_script("obelisk:webhook-bootstrap", WEBHOOK_BOOTSTRAP)
        .map_err(|err| NativeWebhookFailure::CannotInstantiate(err.to_string()))?;
    runtime
        .execute_script(
            "obelisk:crypto-bootstrap",
            crate::v8_crypto::CRYPTO_BOOTSTRAP,
        )
        .map_err(|err| NativeWebhookFailure::CannotInstantiate(err.to_string()))?;
    let entry = loader.specifier_for_path(entry_path).ok_or_else(|| {
        NativeWebhookFailure::CannotInstantiate("JavaScript entry module was not found".into())
    })?;
    let main = ModuleSpecifier::parse("obelisk-main:webhook")
        .map_err(|err| NativeWebhookFailure::CannotInstantiate(err.to_string()))?;
    let source = format!(
        "import handler from {}; globalThis.__obeliskResponse = await globalThis.__obeliskInvoke(handler, {});",
        serde_json::to_string(entry.as_str()).expect("URL must serialize"),
        serde_json::to_string(&request_to_json(request)).expect("request must serialize")
    );
    let evaluated = async {
        let id = runtime.load_main_es_module_from_code(&main, source).await?;
        // Stop once the handler and its body pump finish, abandoning orphaned timers and reads.
        let evaluation = std::pin::pin!(runtime.mod_evaluate(id));
        runtime
            .with_event_loop_promise(evaluation, deno_core::PollEventLoopOptions::default())
            .await
    }
    .await;
    if let Some(reason) = panic.take_trap() {
        return Err(NativeWebhookFailure::Execution(reason));
    }
    if let Err(err) = evaluated {
        let reason = err.to_string();
        if reason.contains("does not provide an export named 'default'") {
            return Err(NativeWebhookFailure::CannotInstantiate(
                "No default export found".into(),
            ));
        }
        return Err(NativeWebhookFailure::Execution(reason));
    }
    if responded.get() {
        return Ok(None);
    }
    let value = runtime
        .execute_script("obelisk:webhook-response", "globalThis.__obeliskResponse")
        .map_err(|err| NativeWebhookFailure::Execution(err.to_string()))?;
    deno_core::scope!(scope, runtime);
    let local = deno_core::v8::Local::new(scope, value);
    deno_core::serde_v8::from_v8::<NativeResponse>(scope, local)
        .map(Some)
        .map_err(|err| NativeWebhookFailure::Execution(err.to_string()))
}

fn request_to_json(request: NativeRequest) -> Value {
    json!({
        "method": request.method,
        "url": request.url,
        "headers": request.headers,
        "body": request.body,
    })
}

pub(super) fn into_hyper_response(
    response: NativeResponse,
) -> Result<Response<HyperOutgoingBody>, String> {
    hyper_response(
        NativeResponseHead {
            status: response.status,
            status_text: response.status_text,
            headers: response.headers,
        },
        UnsyncBoxBody::new(http_body_util::BodyExt::map_err(
            http_body_util::Full::new(Bytes::from(response.body)),
            |_| unreachable!(),
        )),
    )
}

fn hyper_response(
    response: NativeResponseHead,
    body: HyperOutgoingBody,
) -> Result<Response<HyperOutgoingBody>, String> {
    let mut headers = HeaderMap::new();
    for (name, value) in response.headers {
        let name = name
            .parse::<hyper::header::HeaderName>()
            .map_err(|e| e.to_string())?;
        let value = value
            .parse::<hyper::header::HeaderValue>()
            .map_err(|e| e.to_string())?;
        headers.append(name, value);
    }
    let mut result = Response::new(body);
    let status = StatusCode::from_u16(response.status).map_err(|e| e.to_string())?;
    // `""` is the Fetch default, so treat it as unset and let hyper write the canonical phrase.
    if !response.status_text.is_empty() {
        let reason = response
            .status_text
            .chars()
            .map(|char| u8::try_from(char).map_err(|_| "statusText must be a ByteString"))
            .collect::<Result<Vec<_>, _>>()?;
        let reason = hyper::ext::ReasonPhrase::try_from(reason).map_err(|e| e.to_string())?;
        result.extensions_mut().insert(reason);
    }
    *result.status_mut() = status;
    *result.headers_mut() = headers;
    Ok(result)
}

async fn dispatch_host(
    ctx: &mut WebhookEndpointCtx,
    op: &str,
    args: &Value,
    backtrace: Option<concepts::storage::WasmBacktrace>,
) -> Result<Value, JsErrorBox> {
    use types::obelisk::webhook::webhook_dynamic_support::{ExecutionId, Function};
    use types::obelisk::webhook::webhook_support::ExecutionId as SupportExecutionId;
    match op {
        "executionIdGenerate" => Ok(Value::String(concepts::ExecutionId::generate().to_string())),
        "executionIdCurrent" => Ok(Value::String(ctx.execution_id.to_string())),
        "call" => {
            let target = string_arg(args, "target")?;
            let (interface_name, function_name) = split_target(target)?;
            let params = serde_json::to_string(args.get("params").unwrap_or(&Value::Null))
                .map_err(JsErrorBox::from_err)?;
            let outcome = ctx
                .call_json_inner(
                    Function {
                        interface_name,
                        function_name,
                    },
                    params,
                    backtrace,
                )
                .await
                .map_err(|err| JsErrorBox::generic(err.to_string()))?;
            let child_id = ctx.last_direct_call_id.as_ref().map(ToString::to_string);
            let failure_kind = if outcome.is_err() {
                if let Some(child_id) = &child_id {
                    ctx.get_execution_failure_kind(SupportExecutionId {
                        id: child_id.clone(),
                    })
                    .await
                    .ok()
                    .flatten()
                    .map(failure_kind_string)
                } else {
                    None
                }
            } else {
                None
            };
            Ok(outcome_envelope(outcome, child_id, failure_kind))
        }
        "schedule" => {
            let target = string_arg(args, "target")?;
            let (interface_name, function_name) = split_target(target)?;
            let execution_id = if args.get("executionId").is_some() {
                string_arg(args, "executionId")?.to_owned()
            } else {
                concepts::ExecutionId::generate().to_string()
            };
            let params = serde_json::to_string(args.get("params").unwrap_or(&Value::Null))
                .map_err(JsErrorBox::from_err)?;
            ctx.schedule_json_inner(
                ExecutionId {
                    id: execution_id.clone(),
                },
                schedule_arg(args.get("schedule"))?,
                Function {
                    interface_name,
                    function_name,
                },
                params,
                backtrace,
            )
            .await
            .map_err(|err| JsErrorBox::generic(err.to_string()))?;
            Ok(Value::String(execution_id))
        }
        "getStatus" => {
            let id = string_arg(args, "executionId")?.to_owned();
            let status = ctx
                .get_status(SupportExecutionId { id })
                .await
                .map_err(|err| JsErrorBox::generic(err.to_string()))?;
            Ok(status_json(status))
        }
        "get" => get_result(ctx, args, false).await,
        "tryGet" => get_result(ctx, args, true).await,
        _ => Err(JsErrorBox::type_error(format!("unknown webhook op: {op}"))),
    }
}

async fn get_result(
    ctx: &mut WebhookEndpointCtx,
    args: &Value,
    nonblocking: bool,
) -> Result<Value, JsErrorBox> {
    let id = string_arg(args, "executionId")?.to_owned();
    let execution_id = types::obelisk::webhook::webhook_support::ExecutionId { id: id.clone() };
    let outcome = if nonblocking {
        match ctx.try_get_inner(execution_id).await {
            Ok(value) => value,
            Err(types::TryGetErrorTrappable::Normal(
                types::obelisk::webhook::webhook_support::TryGetError::NotFinishedYet,
            )) => {
                return Ok(json!({"pending": true}));
            }
            Err(err) => return Err(JsErrorBox::generic(err.to_string())),
        }
    } else {
        ctx.get_inner(execution_id)
            .await
            .map_err(|err| JsErrorBox::generic(err.to_string()))?
    };
    let failure_kind = if outcome.is_err() {
        ctx.get_execution_failure_kind(types::obelisk::webhook::webhook_support::ExecutionId {
            id: id.clone(),
        })
        .await
        .ok()
        .flatten()
        .map(failure_kind_string)
    } else {
        None
    };
    Ok(outcome_envelope(outcome, Some(id), failure_kind))
}

fn outcome_envelope(
    outcome: Result<Option<String>, Option<String>>,
    child_id: Option<String>,
    failure_kind: Option<String>,
) -> Value {
    match outcome {
        Ok(Some(value)) => {
            json!({"ok": serde_json::from_str::<Value>(&value).unwrap_or(Value::Null)})
        }
        Ok(None) => json!({"ok": Value::Null}),
        Err(value) => json!({
            "throw": value.as_ref().and_then(|value| serde_json::from_str::<Value>(value).ok()),
            "throwUndefined": value.is_none(),
            "childId": child_id,
            "failureKind": failure_kind,
            "cancelled": failure_kind.as_deref() == Some("cancelled"),
        }),
    }
}

fn status_json(status: types::obelisk::webhook::webhook_support::ExecutionStatus) -> Value {
    use types::obelisk::webhook::webhook_support::{ExecutionStatus, ExecutionStatusFinished};
    match status {
        ExecutionStatus::PendingAt(datetime) => {
            json!({"status":"pendingAt", "pendingAt":{"seconds":datetime.seconds,"nanoseconds":datetime.nanoseconds}})
        }
        ExecutionStatus::Locked => json!({"status":"locked"}),
        ExecutionStatus::Paused => json!({"status":"paused"}),
        ExecutionStatus::BlockedByJoinSet => json!({"status":"blockedByJoinSet"}),
        ExecutionStatus::Cancelling => json!({"status":"cancelling"}),
        ExecutionStatus::Finished(finished) => {
            json!({"status":"finished", "finishedStatus": match finished {
                ExecutionStatusFinished::Ok => "ok",
                ExecutionStatusFinished::Err => "err",
                ExecutionStatusFinished::ExecutionFailure(_) => "executionFailure",
            }})
        }
    }
}

fn failure_kind_string(kind: types::obelisk::types::execution::ExecutionFailureKind) -> String {
    use types::obelisk::types::execution::ExecutionFailureKind;
    match kind {
        ExecutionFailureKind::TimedOut => "timed-out",
        ExecutionFailureKind::NondeterminismDetected => "nondeterminism-detected",
        ExecutionFailureKind::OutOfFuel => "out-of-fuel",
        ExecutionFailureKind::Cancelled => "cancelled",
        ExecutionFailureKind::ValueTooLarge => "value-too-large",
        ExecutionFailureKind::Uncategorized => "uncategorized",
    }
    .to_owned()
}

fn string_arg<'a>(args: &'a Value, name: &str) -> Result<&'a str, JsErrorBox> {
    args.get(name)
        .and_then(Value::as_str)
        .ok_or_else(|| JsErrorBox::type_error(format!("{name} must be a string")))
}

fn split_target(target: &str) -> Result<(String, String), JsErrorBox> {
    target
        .rsplit_once('.')
        .map(|(interface, function)| (interface.to_owned(), function.to_owned()))
        .ok_or_else(|| JsErrorBox::type_error("target must contain an interface and function"))
}

fn schedule_arg(
    schedule: Option<&Value>,
) -> Result<types::obelisk::webhook::webhook_support::ScheduleAt, JsErrorBox> {
    use types::obelisk::{types::time::Duration, webhook::webhook_support::ScheduleAt};
    let Some(schedule) = schedule.filter(|value| !value.is_null()) else {
        return Ok(ScheduleAt::Now);
    };
    if let Some(millis) = schedule.get("atMillis").and_then(Value::as_f64) {
        #[expect(
            clippy::cast_possible_truncation,
            clippy::cast_sign_loss,
            reason = "Date.getTime() returns integral milliseconds within the u64 range after clamping negatives"
        )]
        let millis = millis.max(0.0) as u64;
        return Ok(ScheduleAt::At(types::obelisk::types::time::Datetime {
            seconds: millis / 1000,
            nanoseconds: u32::try_from(millis % 1000).unwrap() * 1_000_000,
        }));
    }
    if let Some(milliseconds) = schedule.get("milliseconds").and_then(Value::as_u64) {
        return Ok(ScheduleAt::In(Duration::Milliseconds(milliseconds)));
    }
    if let Some(seconds) = schedule.get("seconds").and_then(Value::as_u64) {
        return Ok(ScheduleAt::In(Duration::Seconds(seconds)));
    }
    if let Some(minutes) = schedule.get("minutes").and_then(Value::as_u64) {
        return Ok(ScheduleAt::In(Duration::Minutes(
            minutes
                .try_into()
                .map_err(|_| JsErrorBox::type_error("minutes out of range"))?,
        )));
    }
    if let Some(hours) = schedule.get("hours").and_then(Value::as_u64) {
        return Ok(ScheduleAt::In(Duration::Hours(
            hours
                .try_into()
                .map_err(|_| JsErrorBox::type_error("hours out of range"))?,
        )));
    }
    if let Some(days) = schedule.get("days").and_then(Value::as_u64) {
        return Ok(ScheduleAt::In(Duration::Days(
            days.try_into()
                .map_err(|_| JsErrorBox::type_error("days out of range"))?,
        )));
    }
    if schedule.get("executionId").is_some() {
        return Ok(ScheduleAt::Now);
    }
    Err(JsErrorBox::type_error("unsupported schedule"))
}

struct InMemoryModuleLoader {
    sources: HashMap<String, String>,
    paths: HashMap<String, ModuleSpecifier>,
}

impl InMemoryModuleLoader {
    fn new(
        files: &BTreeMap<String, String>,
        imports: &HashMap<IfcFqnName, Vec<NamedFnImport>>,
        dynamic_enabled: bool,
    ) -> Self {
        let mut sources = HashMap::new();
        let mut paths = HashMap::new();
        for (path, source) in files {
            let specifier = ModuleSpecifier::parse(&format!("file:///obelisk/{path}"))
                .expect("generated module URL must parse");
            sources.insert(specifier.to_string(), source.clone());
            paths.insert(path.clone(), specifier);
        }
        sources.insert("obelisk-internal:webhook".into(), WEBHOOK_MODULE.into());
        sources.insert("obelisk:webhook@1.0.0".into(), WEBHOOK_BASE_MODULE.into());
        if dynamic_enabled {
            sources.insert(
                "obelisk:webhook-dynamic@1.0.0".into(),
                WEBHOOK_DYNAMIC_MODULE.into(),
            );
        }
        for (specifier, functions) in imports {
            sources.insert(
                specifier.to_string(),
                import_module_source(&specifier.to_string(), functions),
            );
        }
        Self { sources, paths }
    }

    fn specifier_for_path(&self, path: &str) -> Option<&ModuleSpecifier> {
        self.paths.get(path)
    }
}

impl ModuleLoader for InMemoryModuleLoader {
    fn resolve(
        &self,
        specifier: &str,
        referrer: &str,
        _kind: ResolutionKind,
    ) -> Result<ModuleSpecifier, JsErrorBox> {
        let resolved = if self.sources.contains_key(specifier) {
            ModuleSpecifier::parse(specifier).map_err(JsErrorBox::from_err)
        } else {
            resolve_import(specifier, referrer).map_err(JsErrorBox::from_err)
        }?;
        if resolved.as_str() == "obelisk-internal:webhook"
            && (referrer.starts_with("file:") || !self.sources.contains_key(referrer))
        {
            return Err(JsErrorBox::type_error(
                "internal module is unavailable to user code",
            ));
        }
        Ok(resolved)
    }

    fn load(
        &self,
        specifier: &ModuleSpecifier,
        _referrer: Option<&ModuleLoadReferrer>,
        _options: ModuleLoadOptions,
    ) -> ModuleLoadResponse {
        ModuleLoadResponse::Sync(
            self.sources
                .get(specifier.as_str())
                .cloned()
                .ok_or_else(|| JsErrorBox::generic(format!("module not found: {specifier}")))
                .map(|source| {
                    ModuleSource::new(
                        ModuleType::JavaScript,
                        ModuleSourceCode::String(source.into()),
                        specifier,
                        None,
                    )
                }),
        )
    }
}

fn import_module_source(specifier: &str, functions: &[NamedFnImport]) -> String {
    use std::fmt::Write as _;
    use worker_common::js_imports::{SCHEDULE_SUFFIX, strip_specifier_suffix};

    let schedule_base = strip_specifier_suffix(specifier, SCHEDULE_SUFFIX);
    let exports = functions.iter().enumerate().fold(String::new(), |mut exports, (index, function)| {
        let (target, body) = if let Some(base) = &schedule_base {
            let with_id = function.wit_name.ends_with("-schedule-with-id");
            let name = function.wit_name.strip_suffix("-schedule-with-id").or_else(|| function.wit_name.strip_suffix("-schedule")).expect("validated schedule import");
            (format!("{base}.{name}"), if with_id {
                "(executionId, schedule, ...params) => { if (typeof executionId !== 'string') throw new TypeError('executionId must be a string'); return host('schedule', { target: TARGET, schedule, params, executionId }); }".to_owned()
            } else {
                "(schedule, ...params) => host('schedule', { target: TARGET, schedule, params, executionId: schedule?.executionId })".to_owned()
            })
        } else {
            (format!("{specifier}.{}", function.wit_name), "(...params) => unwrap(host('call', { target: TARGET, params }))".to_owned())
        };
        let binding = format!("__obeliskImport{index}");
        writeln!(exports, "const {binding}Target = {}; const {binding} = {}; export {{ {binding} as {} }};", serde_json::to_string(&target).unwrap(), body.replace("TARGET", &format!("{binding}Target")), function.js_name).unwrap();
        exports
    });
    format!("import {{ host, unwrap }} from 'obelisk-internal:webhook';\n{exports}")
}

const WEBHOOK_MODULE: &str = r"
export const host = (op, args = {}) => { if (args.schedule instanceof Date) args = { ...args, schedule: { atMillis: args.schedule.getTime() } }; return Deno.core.ops.op_webhook_host({ op, args: { ...args, __stack: new Error().stack } }); };
export class ChildError extends Error { constructor(value, options = {}) { super(options.message ?? 'child execution failed'); this.value = value; this.childId = options.childId; this.failureKind = options.failureKind; this.cancelled = options.cancelled ?? false; } }
export const unwrap = result => { if ('ok' in result) return result.ok; if (result.pending) return undefined; throw new ChildError(result.throwUndefined ? undefined : result.throw, result); };
export const executionIdGenerate = () => host('executionIdGenerate');
export const executionIdCurrent = () => host('executionIdCurrent');
export const getStatus = executionId => host('getStatus', { executionId });
export const get = executionId => unwrap(host('get', { executionId }));
export const tryGet = executionId => unwrap(host('tryGet', { executionId }));
";

const WEBHOOK_BASE_MODULE: &str = r"
export { executionIdGenerate, executionIdCurrent, getStatus, get, tryGet, ChildError } from 'obelisk-internal:webhook';
";

const WEBHOOK_DYNAMIC_MODULE: &str = r"
import { host, unwrap } from 'obelisk-internal:webhook';
export const call = (target, params) => unwrap(host('call', { target, params }));
export const schedule = (executionId, target, params, schedule) => host('schedule', { executionId, target, params, schedule });
";

const WEBHOOK_BOOTSTRAP: &str = r"
const format = value => typeof value === 'string' ? value : typeof value === 'bigint' ? `${value}n` : JSON.stringify(value);
globalThis.console = Object.fromEntries(['trace', 'debug', 'info', 'log', 'warn', 'error'].map(level => [level, (...values) => Deno.core.ops.op_webhook_log(level === 'log' ? 'info' : level, values.map(format).join(' '))]));
globalThis.process = { env: new Proxy({}, { get: (_, name) => { if (typeof name !== 'string') return undefined; const value=Deno.core.ops.op_webhook_env(name); return value === null ? undefined : value; } }) };
const timers = new Map(); let nextTimer = 1;
globalThis.setTimeout = (callback, milliseconds = 0, ...args) => { const id = nextTimer++; const promise = Deno.core.ops.op_webhook_sleep(Math.max(0, Number(milliseconds) || 0)).then(() => { if (timers.delete(id)) callback(...args); }); timers.set(id, promise); return id; };
globalThis.clearTimeout = id => timers.delete(id);
globalThis.__obeliskInvoke = async (handler, data) => { const request=new Request({__native:true,url:data.url,method:data.method,headers:data.headers,_body:data.body}); const response=await handler(request); if(!(response instanceof Response)) throw new TypeError('handler must return a Response (e.g. `new Response(...)` or `Response.json(...)`)'); const head={status:response.status,statusText:response.statusText,headers:[...response.headers]}; if (response._stream === null && response._rid === null) return {...head,body:await response.text()};
  const reader=response.body.getReader(); Deno.core.ops.op_webhook_respond_stream(head);
  try { let open=true; for (let next=await reader.read(); open && !next.done; next=open ? await reader.read() : next) { const bytes=toBytes(next.value); if (bytes.length && !await Deno.core.ops.op_webhook_body_write(bytes)) { open=false; await reader.cancel(); } } }
  catch (err) { await Deno.core.ops.op_webhook_body_abort(String(err)); throw err; }
  await Deno.core.ops.op_webhook_body_close(); return undefined; };
";
