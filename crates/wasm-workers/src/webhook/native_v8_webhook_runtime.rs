#![allow(clippy::needless_pass_by_value)]

use super::webhook_trigger::{WebhookEndpointCtx, WebhookEndpointJsConfig, types};
use crate::component_logger::log_activities::obelisk::log::log::Host as LogHost;
use crate::js_imports::NamedFnImport;
use concepts::IfcFqnName;
use deno_core::{
    JsRuntime, ModuleLoadOptions, ModuleLoadReferrer, ModuleLoadResponse, ModuleLoader,
    ModuleSource, ModuleSourceCode, ModuleSpecifier, ModuleType, OpState, ResolutionKind,
    RuntimeOptions, op2, resolve_import,
};
use deno_error::JsErrorBox;
use hmac::{Hmac, Mac as _};
use http_body_util::combinators::UnsyncBoxBody;
use hyper::{HeaderMap, Response, StatusCode, body::Bytes};
use rand::RngCore as _;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Sha256, Sha384, Sha512};
use std::{
    cell::RefCell,
    collections::{BTreeMap, HashMap},
    rc::Rc,
};
use types::obelisk::webhook::webhook_support::Host as WebhookSupportHost;
use wasmtime_wasi_http::p2::body::HyperOutgoingBody;

pub(super) struct NativeRequest {
    pub method: String,
    pub url: String,
    pub headers: Vec<(String, String)>,
    pub body: String,
}

pub(super) struct NativeResponse {
    pub status: u16,
    pub headers: Vec<(String, String)>,
    pub body: String,
}

pub(super) enum NativeWebhookFailure {
    CannotInstantiate(String),
    Execution(String),
}

struct HostState {
    ctx: usize,
    handle: tokio::runtime::Handle,
    env: HashMap<String, String>,
    panic: crate::v8_panic::V8PanicState,
}

impl HostState {
    fn ctx(&mut self) -> &mut WebhookEndpointCtx {
        // SAFETY: `execute` owns the context until after the V8 runtime is dropped.
        unsafe { &mut *(self.ctx as *mut WebhookEndpointCtx) }
    }

    fn block_on<T>(&mut self, future: impl Future<Output = T>) -> T {
        let _guard = self.handle.enter();
        futures_lite::future::block_on(future)
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct FetchRequest {
    url: String,
    method: String,
    #[serde(default)]
    headers: Vec<(String, String)>,
    body: Option<String>,
}

#[derive(Serialize)]
struct FetchResponse {
    status: u16,
    headers: Vec<(String, String)>,
    body: String,
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
    let ctx = std::ptr::from_mut::<WebhookEndpointCtx>(host.ctx());
    let future = async move {
        // SAFETY: host calls are serialized by the isolate.
        let ctx = unsafe { &mut *ctx };
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

#[op2(async(deferred))]
#[serde]
async fn op_webhook_fetch(
    state: Rc<RefCell<OpState>>,
    #[serde] request: FetchRequest,
) -> Result<FetchResponse, JsErrorBox> {
    let panic = state.borrow().borrow::<HostState>().panic.clone();
    panic
        .catch_async(op_webhook_fetch_inner(state, request))
        .await
}

async fn op_webhook_fetch_inner(
    state: Rc<RefCell<OpState>>,
    request: FetchRequest,
) -> Result<FetchResponse, JsErrorBox> {
    let ctx = state.borrow_mut().borrow_mut::<HostState>().ctx;
    // SAFETY: host calls are serialized by the isolate.
    let ctx = unsafe { &mut *(ctx as *mut WebhookEndpointCtx) };
    let method = request
        .method
        .parse()
        .map_err(|err| JsErrorBox::type_error(format!("invalid HTTP method: {err}")))?;
    let uri = request
        .url
        .parse()
        .map_err(|err| JsErrorBox::type_error(format!("invalid URL: {err}")))?;
    let (status, headers, body) = ctx
        .http_hooks
        .send_native_request(
            method,
            uri,
            request.headers,
            request.body.unwrap_or_default().into_bytes(),
        )
        .await
        .map_err(JsErrorBox::generic)?;
    Ok(FetchResponse {
        status,
        headers,
        body: String::from_utf8_lossy(&body).into_owned(),
    })
}

#[op2]
#[serde]
fn op_webhook_host(
    state: &mut OpState,
    #[serde] request: HostRequest,
) -> Result<serde_json::Value, JsErrorBox> {
    let panic = state.borrow::<HostState>().panic.clone();
    panic.catch(|| op_webhook_host_inner(state, request))
}

fn op_webhook_host_inner(
    state: &mut OpState,
    request: HostRequest,
) -> Result<serde_json::Value, JsErrorBox> {
    let host = state.borrow_mut::<HostState>();
    let ctx = std::ptr::from_mut::<WebhookEndpointCtx>(host.ctx());
    let future = async move {
        // SAFETY: host calls are serialized by the isolate.
        let ctx = unsafe { &mut *ctx };
        dispatch_host(ctx, &request.op, &request.args).await
    };
    host.block_on(future)
}

#[op2]
#[serde]
fn op_webhook_random(state: &mut OpState, #[smi] length: u32) -> Result<Vec<u8>, JsErrorBox> {
    let panic = state.borrow::<HostState>().panic.clone();
    panic.catch(|| {
        let mut bytes = vec![0; length as usize];
        rand::rng().fill_bytes(&mut bytes);
        Ok(bytes)
    })
}

#[op2]
#[serde]
fn op_webhook_hmac(
    state: &mut OpState,
    #[string] hash: String,
    #[serde] key: Vec<u8>,
    #[serde] message: Vec<u8>,
) -> Result<Vec<u8>, JsErrorBox> {
    let panic = state.borrow::<HostState>().panic.clone();
    panic.catch(|| op_webhook_hmac_inner(hash, key, message))
}

fn op_webhook_hmac_inner(
    hash: String,
    key: Vec<u8>,
    message: Vec<u8>,
) -> Result<Vec<u8>, JsErrorBox> {
    macro_rules! sign {
        ($digest:ty) => {{
            let mut mac = Hmac::<$digest>::new_from_slice(&key)
                .map_err(|err| JsErrorBox::type_error(err.to_string()))?;
            mac.update(&message);
            Ok(mac.finalize().into_bytes().to_vec())
        }};
    }
    match hash.to_ascii_uppercase().as_str() {
        "SHA-256" | "SHA256" => sign!(Sha256),
        "SHA-384" | "SHA384" => sign!(Sha384),
        "SHA-512" | "SHA512" => sign!(Sha512),
        _ => Err(JsErrorBox::type_error(format!(
            "unsupported HMAC hash algorithm: {hash}"
        ))),
    }
}

deno_core::extension!(
    obelisk_webhook_v8,
    ops = [
        op_webhook_log,
        op_webhook_env,
        op_webhook_sleep,
        op_webhook_fetch,
        op_webhook_host,
        op_webhook_random,
        op_webhook_hmac
    ]
);

#[expect(clippy::too_many_arguments)]
pub(super) async fn execute(
    config: &WebhookEndpointJsConfig,
    imports: &HashMap<IfcFqnName, Vec<NamedFnImport>>,
    request: NativeRequest,
    env: HashMap<String, String>,
    ctx: &mut WebhookEndpointCtx,
    handle: tokio::runtime::Handle,
    isolate_tx: tokio::sync::oneshot::Sender<deno_core::v8::IsolateHandle>,
    max_heap_size: usize,
) -> Result<NativeResponse, NativeWebhookFailure> {
    let loader = Rc::new(InMemoryModuleLoader::new(&config.files, imports));
    let mut runtime = JsRuntime::new(RuntimeOptions {
        module_loader: Some(loader.clone()),
        extensions: vec![obelisk_webhook_v8::init()],
        create_params: Some(deno_core::v8::CreateParams::default().heap_limits(0, max_heap_size)),
        ..Default::default()
    });
    let isolate = runtime.v8_isolate().thread_safe_handle();
    let _ = isolate_tx.send(isolate.clone());
    let panic = crate::v8_panic::V8PanicState::new(isolate);
    runtime.op_state().borrow_mut().put(HostState {
        ctx: std::ptr::from_mut(ctx) as usize,
        handle,
        env,
        panic: panic.clone(),
    });
    runtime
        .execute_script("obelisk:webhook-bootstrap", WEBHOOK_BOOTSTRAP)
        .map_err(|err| NativeWebhookFailure::CannotInstantiate(err.to_string()))?;
    let entry = loader
        .specifier_for_path(&config.entry_path)
        .ok_or_else(|| {
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
        let evaluation = runtime.mod_evaluate(id);
        runtime
            .run_event_loop(deno_core::PollEventLoopOptions::default())
            .await?;
        evaluation.await
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
    let value = runtime
        .execute_script("obelisk:webhook-response", "globalThis.__obeliskResponse")
        .map_err(|err| NativeWebhookFailure::Execution(err.to_string()))?;
    let response = {
        deno_core::scope!(scope, runtime);
        let local = deno_core::v8::Local::new(scope, value);
        deno_core::serde_v8::from_v8::<NativeResponseSerde>(scope, local)
            .map_err(|err| NativeWebhookFailure::Execution(err.to_string()))?
    };
    Ok(NativeResponse {
        status: response.status,
        headers: response.headers,
        body: response.body,
    })
}

#[derive(Deserialize)]
struct NativeResponseSerde {
    status: u16,
    headers: Vec<(String, String)>,
    body: String,
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
    let mut result = Response::new(UnsyncBoxBody::new(http_body_util::BodyExt::map_err(
        http_body_util::Full::new(Bytes::from(response.body)),
        |_| unreachable!(),
    )));
    *result.status_mut() = StatusCode::from_u16(response.status).map_err(|e| e.to_string())?;
    *result.headers_mut() = headers;
    Ok(result)
}

async fn dispatch_host(
    ctx: &mut WebhookEndpointCtx,
    op: &str,
    args: &Value,
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
                    native_backtrace(ctx, args),
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
            let execution_id = args
                .get("executionId")
                .and_then(Value::as_str)
                .map(ToOwned::to_owned)
                .unwrap_or_else(|| concepts::ExecutionId::generate().to_string());
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
                native_backtrace(ctx, args),
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

fn native_backtrace(
    ctx: &WebhookEndpointCtx,
    _args: &Value,
) -> Option<concepts::storage::WasmBacktrace> {
    ctx.backtrace_persist
        .then(|| concepts::storage::WasmBacktrace {
            frames: vec![concepts::storage::FrameInfo {
                module: "native-v8".to_owned(),
                func_name: "javascript".to_owned(),
                symbols: vec![concepts::storage::FrameSymbol {
                    func_name: None,
                    file: Some("javascript".to_owned()),
                    line: None,
                    col: None,
                }],
            }],
        })
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
    ) -> Self {
        let mut sources = HashMap::new();
        let mut paths = HashMap::new();
        for (path, source) in files {
            let specifier = ModuleSpecifier::parse(&format!("file:///obelisk/{path}"))
                .expect("generated module URL must parse");
            sources.insert(specifier.to_string(), source.clone());
            paths.insert(path.clone(), specifier);
        }
        sources.insert("obelisk:webhook@1.0.0".into(), WEBHOOK_MODULE.into());
        sources.insert(
            "obelisk:webhook-dynamic@1.0.0".into(),
            WEBHOOK_DYNAMIC_MODULE.into(),
        );
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
        if self.sources.contains_key(specifier) {
            ModuleSpecifier::parse(specifier).map_err(JsErrorBox::from_err)
        } else {
            resolve_import(specifier, referrer).map_err(JsErrorBox::from_err)
        }
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
    use boa_common::imports::{SCHEDULE_SUFFIX, strip_specifier_suffix};
    use std::fmt::Write as _;

    let schedule_base = strip_specifier_suffix(specifier, SCHEDULE_SUFFIX);
    let exports = functions.iter().enumerate().fold(String::new(), |mut exports, (index, function)| {
        let (target, body) = if let Some(base) = &schedule_base {
            let name = function.wit_name.strip_suffix("-schedule").expect("validated schedule import");
            (format!("{base}.{name}"), "(schedule, ...params) => host('schedule', { target: TARGET, schedule, params })".to_owned())
        } else {
            (format!("{specifier}.{}", function.wit_name), "(...params) => unwrap(host('call', { target: TARGET, params }))".to_owned())
        };
        let binding = format!("__obeliskImport{index}");
        writeln!(exports, "const {binding}Target = {}; const {binding} = {}; export {{ {binding} as {} }};", serde_json::to_string(&target).unwrap(), body.replace("TARGET", &format!("{binding}Target")), function.js_name).unwrap();
        exports
    });
    format!(
        "import 'obelisk:webhook@1.0.0'; const host=globalThis.__obeliskHost; const unwrap=globalThis.__obeliskUnwrap;\n{exports}"
    )
}

const WEBHOOK_MODULE: &str = r"
const host = (op, args = {}) => Deno.core.ops.op_webhook_host({ op, args: { ...args, __stack: new Error().stack } });
export class ChildError extends Error { constructor(value, options = {}) { super(options.message ?? 'child execution failed'); this.value = value; this.childId = options.childId; this.failureKind = options.failureKind; this.cancelled = options.cancelled ?? false; } }
const unwrap = result => { if ('ok' in result) return result.ok; if (result.pending) return undefined; throw new ChildError(result.throwUndefined ? undefined : result.throw, result); };
export const executionIdGenerate = () => host('executionIdGenerate');
export const executionIdCurrent = () => host('executionIdCurrent');
export const getStatus = executionId => host('getStatus', { executionId });
export const get = executionId => unwrap(host('get', { executionId }));
export const tryGet = executionId => unwrap(host('tryGet', { executionId }));
globalThis.__obeliskHost = host; globalThis.__obeliskUnwrap = unwrap; globalThis.__obeliskChildError = ChildError;
";

const WEBHOOK_DYNAMIC_MODULE: &str = r"
import 'obelisk:webhook@1.0.0';
const host = globalThis.__obeliskHost;
export const call = (target, params) => globalThis.__obeliskUnwrap(host('call', { target, params }));
export const schedule = (executionId, target, params, schedule) => host('schedule', { executionId, target, params, schedule });
";

const WEBHOOK_BOOTSTRAP: &str = r"
const format = value => typeof value === 'string' ? value : typeof value === 'bigint' ? `${value}n` : JSON.stringify(value);
globalThis.console = Object.fromEntries(['trace', 'debug', 'info', 'log', 'warn', 'error'].map(level => [level, (...values) => Deno.core.ops.op_webhook_log(level === 'log' ? 'info' : level, values.map(format).join(' '))]));
globalThis.process = { env: new Proxy({}, { get: (_, name) => { if (typeof name !== 'string') return undefined; const value=Deno.core.ops.op_webhook_env(name); return value === null ? undefined : value; } }) };
const timers = new Map(); let nextTimer = 1;
globalThis.setTimeout = (callback, milliseconds = 0, ...args) => { const id = nextTimer++; const promise = Deno.core.ops.op_webhook_sleep(Math.max(0, Number(milliseconds) || 0)).then(() => { if (timers.delete(id)) callback(...args); }); timers.set(id, promise); return id; };
globalThis.clearTimeout = id => timers.delete(id);
const decode = value => decodeURIComponent(value.replace(/\+/g, ' '));
const encode = value => encodeURIComponent(String(value)).replace(/%20/g, '+');
class URLSearchParams {
  constructor(input='', update=null) { this._pairs=[]; this._update=update; for(const part of String(input).replace(/^\?/, '').split('&')) { if(part) { const at=part.indexOf('='); this._pairs.push(at<0?[decode(part),'']:[decode(part.slice(0,at)),decode(part.slice(at+1))]); } } }
  _changed(){if(this._update)this._update(this.toString());} append(name,value){this._pairs.push([String(name),String(value)]);this._changed();} set(name,value){name=String(name);const first=this._pairs.findIndex(pair=>pair[0]===name);this._pairs=this._pairs.filter(pair=>pair[0]!==name);this._pairs.splice(first<0?this._pairs.length:first,0,[name,String(value)]);this._changed();} get(name){const pair=this._pairs.find(pair=>pair[0]===String(name));return pair?pair[1]:null;} getAll(name){return this._pairs.filter(pair=>pair[0]===String(name)).map(pair=>pair[1]);} has(name){return this._pairs.some(pair=>pair[0]===String(name));} delete(name){this._pairs=this._pairs.filter(pair=>pair[0]!==String(name));this._changed();} sort(){this._pairs=this._pairs.map((pair,index)=>[pair,index]).sort((a,b)=>a[0][0].localeCompare(b[0][0])||a[1]-b[1]).map(item=>item[0]);this._changed();} get size(){return this._pairs.length;} toString(){return this._pairs.map(pair=>`${encode(pair[0])}=${encode(pair[1])}`).join('&');} [Symbol.iterator](){return this._pairs[Symbol.iterator]();}
}
class URL { constructor(input,base){const value=String(input);const match=/^(https?):\/\/([^/?#]+)([^?#]*)(?:\?([^#]*))?(?:#(.*))?$/.exec(value);if(!match)throw new TypeError(`Invalid URL: ${value}`);this.protocol=`${match[1]}:`;this.host=match[2];this.hostname=this.host.split(':')[0];this.pathname=match[3]||'/';this.hash=match[5]?`#${match[5]}`:'';this.searchParams=new URLSearchParams(match[4]||'',query=>{this.search=query?`?${query}`:'';});this.search=match[4]?`?${match[4]}`:'';} get origin(){return `${this.protocol}//${this.host}`;} get href(){return `${this.origin}${this.pathname}${this.search}${this.hash}`;} toString(){return this.href;} }
globalThis.URLSearchParams = URLSearchParams;
globalThis.URL = URL;
class TextEncoder { encode(value = '') { const encoded = unescape(encodeURIComponent(String(value))); return Uint8Array.from(encoded, char => char.charCodeAt(0)); } }
class TextDecoder { decode(value = new Uint8Array()) { const bytes = value instanceof Uint8Array ? value : new Uint8Array(value); return decodeURIComponent(escape(String.fromCharCode(...bytes))); } }
globalThis.TextEncoder = TextEncoder; globalThis.TextDecoder = TextDecoder;
const hmacHash = key => typeof key.algorithm.hash === 'string' ? key.algorithm.hash : key.algorithm.hash.name;
const hmacSign = (key,data) => Uint8Array.from(Deno.core.ops.op_webhook_hmac(hmacHash(key),key.bytes,[...new Uint8Array(data)]));
globalThis.crypto = { getRandomValues(array) { const bytes=Deno.core.ops.op_webhook_random(array.byteLength); new Uint8Array(array.buffer,array.byteOffset,array.byteLength).set(bytes); return array; }, subtle: { async importKey(format,keyData,algorithm,extractable,usages) { if(format!=='raw'||String(algorithm.name).toUpperCase()!=='HMAC') throw new TypeError('only raw HMAC keys are supported'); return {bytes:[...new Uint8Array(keyData)],algorithm,usages}; }, async sign(algorithm,key,data) { if(String(typeof algorithm==='string'?algorithm:algorithm.name).toUpperCase()!=='HMAC') throw new TypeError('only HMAC signing is supported'); return hmacSign(key,data).buffer; }, async verify(algorithm,key,signature,data) { if(String(typeof algorithm==='string'?algorithm:algorithm.name).toUpperCase()!=='HMAC') throw new TypeError('only HMAC verification is supported'); const expected=hmacSign(key,data), actual=new Uint8Array(signature); if(expected.length!==actual.length)return false; let difference=0; for(let i=0;i<expected.length;i++)difference|=expected[i]^actual[i]; return difference===0; } } };
class Headers { constructor(init = []) { this.entries = init instanceof Headers ? [...init] : Array.isArray(init) ? init.map(([k,v]) => [String(k).toLowerCase(), String(v)]) : Object.entries(init).map(([k,v]) => [k.toLowerCase(), String(v)]); } get(name) { const values=this.entries.filter(([k]) => k === String(name).toLowerCase()).map(([,v])=>v); return values.length ? values.join(', ') : null; } has(name) { return this.get(name) !== null; } set(name,value) { const key=String(name).toLowerCase(); this.entries=this.entries.filter(([k])=>k!==key); this.entries.push([key,String(value)]); } append(name,value) { this.entries.push([String(name).toLowerCase(),String(value)]); } [Symbol.iterator]() { return this.entries[Symbol.iterator](); } }
globalThis.Headers = Headers;
class Request { constructor(input, options = {}) { if (typeof input === 'object' && input.__native) { Object.assign(this,input); this.headers=new Headers(input.headers); } else { this.url=String(input); this.method=options.method||'GET'; this.headers=new Headers(options.headers); this._body=options.body??''; } } async text(){return this._body??this.body??'';} async json(){return JSON.parse(await this.text());} async formData(){return Object.fromEntries(new URLSearchParams(await this.text()));} }
globalThis.Request = Request;
class Response { constructor(body = '', options = {}) { this._body=body == null ? '' : String(body); this.status=options.status??200; this.ok=this.status>=200&&this.status<300; this.headers=new Headers(options.headers); } async text(){return this._body;} async json(){return JSON.parse(this._body);} static json(value, options = {}) { const response=new Response(JSON.stringify(value),options); if(!response.headers.has('content-type')) response.headers.set('content-type','application/json'); return response; } }
globalThis.Response = Response;
globalThis.fetch = async (input, options = {}) => { const request=input instanceof Request?input:new Request(input,options); const data=await Deno.core.ops.op_webhook_fetch({url:request.url,method:request.method,headers:[...request.headers],body:request._body}); return new Response(data.body,{status:data.status,headers:data.headers}); };
globalThis.__obeliskInvoke = async (handler, data) => { const request=new Request({__native:true,url:data.url,method:data.method,headers:data.headers,_body:data.body}); const response=await handler(request); if(!(response instanceof Response)) throw new TypeError('handler must return a Response (e.g. `new Response(...)` or `Response.json(...)`)'); return {status:response.status,headers:[...response.headers],body:await response.text()}; };
";
