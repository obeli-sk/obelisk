#![allow(clippy::needless_pass_by_value)]

use crate::component_logger::ComponentLogger;
use crate::http_hooks::HttpHooks;
use concepts::storage::LogLevel;
use concepts::{Params, ReturnTypeExtendable, SupportedFunctionReturnValue};
use deno_core::{
    JsRuntime, ModuleLoadOptions, ModuleLoadReferrer, ModuleLoadResponse, ModuleLoader,
    ModuleSource, ModuleSourceCode, ModuleSpecifier, ModuleType, OpState, ResolutionKind,
    RuntimeOptions, op2, resolve_import,
};
use deno_error::JsErrorBox;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::cell::RefCell;
use std::collections::{BTreeMap, HashMap};
use std::rc::Rc;
use std::sync::Arc;
use tokio::sync::Mutex;

pub(crate) struct NativeActivityState {
    pub(crate) logger: ComponentLogger,
    pub(crate) http_hooks: HttpHooks,
    pub(crate) env: HashMap<String, String>,
}

pub(crate) enum NativeActivityFailure {
    CannotInstantiate(String),
    ResultParsing(String),
    Trap(String),
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

struct HostState {
    activity: Arc<Mutex<NativeActivityState>>,
    env: HashMap<String, String>,
}

#[op2(fast)]
fn op_activity_log(state: &mut OpState, #[string] level: String, #[string] message: String) {
    let activity = state.borrow::<HostState>().activity.clone();
    let level = match level.as_str() {
        "trace" => LogLevel::Trace,
        "debug" => LogLevel::Debug,
        "warn" => LogLevel::Warn,
        "error" => LogLevel::Error,
        _ => LogLevel::Info,
    };
    if let Ok(mut activity) = activity.try_lock() {
        activity.logger.log(level, message);
    }
}

#[op2]
#[string]
fn op_activity_env(state: &mut OpState, #[string] name: String) -> Option<String> {
    state.borrow::<HostState>().env.get(&name).cloned()
}

#[op2(async(deferred), fast)]
async fn op_activity_sleep(#[number] milliseconds: u64) {
    tokio::time::sleep(std::time::Duration::from_millis(milliseconds)).await;
}

#[op2(async(deferred))]
#[serde]
async fn op_activity_fetch(
    state: Rc<RefCell<OpState>>,
    #[serde] request: FetchRequest,
) -> Result<FetchResponse, JsErrorBox> {
    let activity = state.borrow().borrow::<HostState>().activity.clone();
    let method = request
        .method
        .parse()
        .map_err(|err| JsErrorBox::type_error(format!("invalid HTTP method: {err}")))?;
    let uri = request
        .url
        .parse()
        .map_err(|err| JsErrorBox::type_error(format!("invalid URL: {err}")))?;
    let mut activity = activity.lock().await;
    let (status, headers, body) = activity
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

deno_core::extension!(
    obelisk_activity_v8,
    ops = [
        op_activity_log,
        op_activity_env,
        op_activity_sleep,
        op_activity_fetch
    ]
);

pub(crate) fn execute(
    entry_path: &str,
    files: &BTreeMap<String, String>,
    params: &Params,
    return_type: &ReturnTypeExtendable,
    state: NativeActivityState,
    isolate_tx: tokio::sync::oneshot::Sender<deno_core::v8::IsolateHandle>,
) -> (
    Result<SupportedFunctionReturnValue, NativeActivityFailure>,
    NativeActivityState,
) {
    let loader = Rc::new(InMemoryModuleLoader::new(files));
    let shared = Arc::new(Mutex::new(state));
    let env = futures_lite::future::block_on(async { shared.lock().await.env.clone() });
    let mut runtime = JsRuntime::new(RuntimeOptions {
        module_loader: Some(loader.clone()),
        extensions: vec![obelisk_activity_v8::init()],
        ..Default::default()
    });
    let _ = isolate_tx.send(runtime.v8_isolate().thread_safe_handle());
    runtime.op_state().borrow_mut().put(HostState {
        activity: shared.clone(),
        env,
    });
    let result = execute_inner(&mut runtime, &loader, entry_path, params, return_type);
    drop(runtime);
    let state = Arc::try_unwrap(shared)
        .unwrap_or_else(|_| panic!("native V8 activity host state is still referenced"))
        .into_inner();
    (result, state)
}

fn execute_inner(
    runtime: &mut JsRuntime,
    loader: &InMemoryModuleLoader,
    entry_path: &str,
    params: &Params,
    return_type: &ReturnTypeExtendable,
) -> Result<SupportedFunctionReturnValue, NativeActivityFailure> {
    runtime
        .execute_script("obelisk:activity-bootstrap", ACTIVITY_BOOTSTRAP)
        .map_err(|err| NativeActivityFailure::CannotInstantiate(err.to_string()))?;
    let params = params.as_json_values().ok_or_else(|| {
        NativeActivityFailure::ResultParsing("parameters are not JSON values".into())
    })?;
    let entry = loader.specifier_for_path(entry_path).ok_or_else(|| {
        NativeActivityFailure::CannotInstantiate("JavaScript entry module was not found".into())
    })?;
    let main = ModuleSpecifier::parse("obelisk-main:run")
        .map_err(|err| NativeActivityFailure::CannotInstantiate(err.to_string()))?;
    let source = format!(
        "import activity from {}; try {{ globalThis.__obeliskResult = {{ ok: true, value: await activity(...{}) }}; }} catch (error) {{ globalThis.__obeliskResult = {{ ok: false, absent: error === undefined, value: error instanceof Error ? error.message : error }}; }} finally {{ globalThis.__obeliskClearTimers(); }}",
        serde_json::to_string(entry.as_str()).expect("URL must serialize"),
        serde_json::to_string(&params).expect("parameters must serialize")
    );
    let evaluated = futures_lite::future::block_on(async {
        let id = runtime.load_main_es_module_from_code(&main, source).await?;
        let evaluation = runtime.mod_evaluate(id);
        runtime.run_event_loop(Default::default()).await?;
        evaluation.await
    });
    if let Err(err) = evaluated {
        let reason = err.to_string();
        if reason.contains("does not provide an export named 'default'") {
            return Err(NativeActivityFailure::CannotInstantiate(format!(
                "no default export: {reason}"
            )));
        }
        if reason.contains("Module parse")
            || reason.contains("SyntaxError")
            || reason.contains("Module not found")
        {
            return Err(NativeActivityFailure::CannotInstantiate(format!(
                "module parse error: {reason}"
            )));
        }
        return Err(NativeActivityFailure::Trap(reason));
    }
    let value = runtime
        .execute_script("obelisk:activity-result", "globalThis.__obeliskResult")
        .map_err(|err| NativeActivityFailure::Trap(err.to_string()))?;
    let envelope = {
        deno_core::scope!(scope, runtime);
        let local = deno_core::v8::Local::new(scope, value);
        deno_core::serde_v8::from_v8::<Value>(scope, local)
            .map_err(|err| NativeActivityFailure::ResultParsing(err.to_string()))?
    };
    let ok = envelope.get("ok").and_then(Value::as_bool).unwrap_or(false);
    let absent = envelope
        .get("absent")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    let value = (!absent).then(|| envelope.get("value").cloned().unwrap_or(Value::Null));
    let mapped = if ok {
        crate::js_worker_utils::map_ok_variant_fatal(
            value,
            return_type,
            concepts::storage::Version(0),
        )
    } else {
        crate::js_worker_utils::map_err_variant_fatal(
            value,
            return_type,
            concepts::storage::Version(0),
        )
    };
    mapped.map_err(|(err, _)| {
        let reason = match err {
            executor::worker::FatalError::ResultParsingError(
                concepts::ResultParsingError::ResultParsingErrorFromVal(
                    concepts::ResultParsingErrorFromVal::TypeCheckError(reason),
                ),
            ) => reason,
            other => other.to_string(),
        };
        NativeActivityFailure::ResultParsing(reason)
    })
}

struct InMemoryModuleLoader {
    sources: HashMap<String, String>,
    paths: HashMap<String, ModuleSpecifier>,
}

impl InMemoryModuleLoader {
    fn new(files: &BTreeMap<String, String>) -> Self {
        let mut sources = HashMap::new();
        let mut paths = HashMap::new();
        for (path, source) in files {
            let specifier = ModuleSpecifier::parse(&format!("file:///obelisk/{path}"))
                .expect("generated module URL must parse");
            sources.insert(specifier.to_string(), source.clone());
            paths.insert(path.clone(), specifier);
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
        resolve_import(specifier, referrer).map_err(JsErrorBox::from_err)
    }

    fn load(
        &self,
        specifier: &ModuleSpecifier,
        _referrer: Option<&ModuleLoadReferrer>,
        _options: ModuleLoadOptions,
    ) -> ModuleLoadResponse {
        let result = self
            .sources
            .get(specifier.as_str())
            .cloned()
            .ok_or_else(|| JsErrorBox::generic(format!("Module not found: {specifier}")))
            .map(|source| {
                ModuleSource::new(
                    ModuleType::JavaScript,
                    ModuleSourceCode::String(source.into()),
                    specifier,
                    None,
                )
            });
        ModuleLoadResponse::Sync(result)
    }
}

const ACTIVITY_BOOTSTRAP: &str = r#"
const format = value => typeof value === 'string' ? value : typeof value === 'bigint' ? `${value}n` : JSON.stringify(value);
globalThis.console = Object.fromEntries(['trace', 'debug', 'info', 'log', 'warn', 'error'].map(level => [level, (...values) => Deno.core.ops.op_activity_log(level === 'log' ? 'info' : level, values.map(format).join(' '))]));
globalThis.process = { env: new Proxy({}, { get: (_, name) => typeof name === 'string' ? Deno.core.ops.op_activity_env(name) : undefined }) };
let nextTimer = 1;
const timers = new Map();
globalThis.setTimeout = (callback, delay = 0, ...args) => { const id = nextTimer++; const token = {}; timers.set(id, token); Deno.core.ops.op_activity_sleep(Math.max(0, Number(delay))).then(() => { if (timers.get(id) === token) { timers.delete(id); callback(...args); } }); return id; };
globalThis.clearTimeout = id => timers.delete(id);
globalThis.__obeliskClearTimers = () => timers.clear();
const decode = value => decodeURIComponent(value.replace(/\+/g, ' '));
const encode = value => encodeURIComponent(String(value)).replace(/%20/g, '+');
class URLSearchParams {
  constructor(input = '', update = null) { this._pairs = []; this._update = update; for (const part of String(input).replace(/^\?/, '').split('&')) { if (part) { const at = part.indexOf('='); this._pairs.push(at < 0 ? [decode(part), ''] : [decode(part.slice(0, at)), decode(part.slice(at + 1))]); } } }
  _changed() { if (this._update) this._update(this.toString()); }
  append(name, value) { this._pairs.push([String(name), String(value)]); this._changed(); }
  set(name, value) { name = String(name); const first = this._pairs.findIndex(pair => pair[0] === name); this._pairs = this._pairs.filter(pair => pair[0] !== name); this._pairs.splice(first < 0 ? this._pairs.length : first, 0, [name, String(value)]); this._changed(); }
  get(name) { const pair = this._pairs.find(pair => pair[0] === String(name)); return pair ? pair[1] : null; }
  getAll(name) { return this._pairs.filter(pair => pair[0] === String(name)).map(pair => pair[1]); }
  has(name) { return this._pairs.some(pair => pair[0] === String(name)); }
  delete(name) { this._pairs = this._pairs.filter(pair => pair[0] !== String(name)); this._changed(); }
  sort() { this._pairs = this._pairs.map((pair, index) => [pair, index]).sort((a, b) => a[0][0].localeCompare(b[0][0]) || a[1] - b[1]).map(item => item[0]); this._changed(); }
  get size() { return this._pairs.length; }
  toString() { return this._pairs.map(pair => `${encode(pair[0])}=${encode(pair[1])}`).join('&'); }
  [Symbol.iterator]() { return this._pairs[Symbol.iterator](); }
}
class URL {
  constructor(input, base) { const value = String(input); const match = /^(https?):\/\/([^/?#]+)([^?#]*)(?:\?([^#]*))?(?:#(.*))?$/.exec(value); if (!match) throw new TypeError(`Invalid URL: ${value}`); this.protocol = `${match[1]}:`; this.host = match[2]; this.hostname = this.host.split(':')[0]; this.pathname = match[3] || '/'; this.hash = match[5] ? `#${match[5]}` : ''; this.searchParams = new URLSearchParams(match[4] || '', query => { this.search = query ? `?${query}` : ''; }); this.search = match[4] ? `?${match[4]}` : ''; }
  get origin() { return `${this.protocol}//${this.host}`; }
  get href() { return `${this.origin}${this.pathname}${this.search}${this.hash}`; }
  toString() { return this.href; }
}
globalThis.URLSearchParams = URLSearchParams;
globalThis.URL = URL;
class Response {
  constructor(data) { this.status = data.status; this.ok = data.status >= 200 && data.status < 300; this.headers = new Map(data.headers); this._body = data.body; }
  async text() { return this._body; }
  async json() { return JSON.parse(this._body); }
}
globalThis.fetch = async (input, options = {}) => {
  const url = typeof input === 'string' ? input : input.url;
  const headers = options.headers ? Object.entries(options.headers) : [];
  const data = await Deno.core.ops.op_activity_fetch({ url, method: options.method || 'GET', headers, body: options.body });
  return new Response(data);
};
"#;
