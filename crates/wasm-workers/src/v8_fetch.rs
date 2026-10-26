//! `fetch` with a streamed response body, shared by V8 activities and webhooks.

use crate::http_hooks::{NativeHttpBody, NativeHttpHead};
use crate::v8_panic::V8PanicState;
use deno_core::{OpState, op2};
use deno_error::JsErrorBox;
use serde::{Deserialize, Serialize};
use std::cell::RefCell;
use std::collections::HashMap;
use std::pin::Pin;
use std::rc::Rc;

pub(crate) type PendingFetch =
    Pin<Box<dyn Future<Output = Result<(NativeHttpHead, NativeHttpBody), String>> + Send>>;

/// Starts an outbound request through the runtime's HTTP hooks.
pub(crate) type StartFetch = Box<
    dyn FnMut(
        hyper::Method,
        hyper::Uri,
        Vec<(String, Vec<u8>)>,
        Vec<u8>,
    ) -> Result<PendingFetch, String>,
>;

/// Per-isolate fetch state; ops expect it and a `V8PanicState` in the `OpState`.
pub(crate) struct FetchHost {
    start: StartFetch,
    bodies: HashMap<u32, NativeHttpBody>,
    next_rid: u32,
}

impl FetchHost {
    pub(crate) fn new(start: StartFetch) -> Self {
        Self {
            start,
            bodies: HashMap::new(),
            next_rid: 0,
        }
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
#[serde(rename_all = "camelCase")]
struct FetchHead {
    status: u16,
    status_text: String,
    headers: Vec<(String, String)>,
    /// Handle of the body for `op_fetch_read`.
    rid: u32,
}

/// Sends the request and resolves once the response headers arrive.
#[op2(async(deferred))]
#[serde]
async fn op_fetch(
    state: Rc<RefCell<OpState>>,
    #[serde] request: FetchRequest,
) -> Result<FetchHead, JsErrorBox> {
    let panic = state.borrow().borrow::<V8PanicState>().clone();
    panic.catch_async(op_fetch_inner(state, request)).await
}

async fn op_fetch_inner(
    state: Rc<RefCell<OpState>>,
    request: FetchRequest,
) -> Result<FetchHead, JsErrorBox> {
    let method = request
        .method
        .parse()
        .map_err(|err| JsErrorBox::type_error(format!("invalid HTTP method: {err}")))?;
    let uri = request
        .url
        .parse()
        .map_err(|err| JsErrorBox::type_error(format!("invalid URL: {err}")))?;
    let headers = crate::http_hooks::native_request_headers(request.headers)
        .map_err(JsErrorBox::type_error)?;
    let pending = {
        let mut state = state.borrow_mut();
        let host = state.borrow_mut::<FetchHost>();
        (host.start)(
            method,
            uri,
            headers,
            request.body.unwrap_or_default().into_bytes(),
        )
        .map_err(JsErrorBox::generic)?
    };
    let (head, body) = pending.await.map_err(JsErrorBox::generic)?;
    let mut state = state.borrow_mut();
    let host = state.borrow_mut::<FetchHost>();
    let rid = host.next_rid;
    host.next_rid += 1;
    host.bodies.insert(rid, body);
    Ok(FetchHead {
        status: head.status,
        status_text: head.status_text,
        headers: head.headers,
        rid,
    })
}

fn take_body(state: &RefCell<OpState>, rid: u32) -> Result<NativeHttpBody, JsErrorBox> {
    state
        .borrow_mut()
        .borrow_mut::<FetchHost>()
        .bodies
        .remove(&rid)
        .ok_or_else(|| JsErrorBox::type_error("fetch response body is already consumed"))
}

/// Next chunk of a fetch response body; empty at the end.
#[op2(async(deferred), fast)]
#[buffer]
async fn op_fetch_read(
    state: Rc<RefCell<OpState>>,
    #[smi] rid: u32,
) -> Result<Vec<u8>, JsErrorBox> {
    let panic = state.borrow().borrow::<V8PanicState>().clone();
    panic
        .catch_async(async move {
            let mut body = take_body(&state, rid)?;
            let chunk = body.next_chunk().await.map_err(JsErrorBox::generic)?;
            Ok(if let Some(chunk) = chunk {
                state
                    .borrow_mut()
                    .borrow_mut::<FetchHost>()
                    .bodies
                    .insert(rid, body);
                chunk.to_vec()
            } else {
                Vec::new()
            })
        })
        .await
}

#[op2(async(deferred), fast)]
#[string]
async fn op_fetch_text(state: Rc<RefCell<OpState>>, #[smi] rid: u32) -> Result<String, JsErrorBox> {
    let panic = state.borrow().borrow::<V8PanicState>().clone();
    panic
        .catch_async(async move {
            let body = take_body(&state, rid)?
                .collect()
                .await
                .map_err(JsErrorBox::generic)?;
            Ok(String::from_utf8_lossy(&body).into_owned())
        })
        .await
}

#[op2(fast)]
fn op_fetch_close(state: &mut OpState, #[smi] rid: u32) {
    state.borrow_mut::<FetchHost>().bodies.remove(&rid);
}

deno_core::extension!(
    obelisk_fetch_v8,
    ops = [op_fetch, op_fetch_read, op_fetch_text, op_fetch_close]
);

/// Defines `TextEncoder`, `TextDecoder`, `ReadableStream`, `Headers`, `Request`, `Response`
/// and `fetch`, plus the script-scope helpers `toBytes` and `concatBytes`.
pub(crate) const FETCH_BOOTSTRAP: &str = r"
const toBytes = value => { if (typeof value === 'string') return Deno.core.encode(value); if (value instanceof Uint8Array) return value; if (ArrayBuffer.isView(value)) return new Uint8Array(value.buffer, value.byteOffset, value.byteLength); if (value instanceof ArrayBuffer) return new Uint8Array(value); throw new TypeError('body chunk must be a string, an ArrayBuffer or an ArrayBufferView'); };
const concatBytes = (a, b) => { const joined = new Uint8Array(a.length + b.length); joined.set(a); joined.set(b, a.length); return joined; };
// Length of an incomplete UTF-8 sequence at the end of `bytes`.
const incompleteTail = bytes => { for (let back = 1; back <= Math.min(3, bytes.length); back++) { const byte = bytes[bytes.length - back]; if ((byte & 0xc0) !== 0x80) { const need = byte >= 0xf0 ? 4 : byte >= 0xe0 ? 3 : byte >= 0xc0 ? 2 : 1; return need > back ? back : 0; } } return 0; };
class TextEncoder { get encoding() { return 'utf-8'; } encode(value = '') { return Deno.core.encode(String(value)); } }
class TextDecoder { constructor(label = 'utf-8') { if (!/^utf-?8$/i.test(String(label))) throw new RangeError(`unsupported encoding: ${label}`); this._pending = new Uint8Array(0); } get encoding() { return 'utf-8'; } decode(value = new Uint8Array(), options = {}) { let bytes = value instanceof ArrayBuffer || ArrayBuffer.isView(value) ? toBytes(value) : new Uint8Array(value); if (this._pending.length) { bytes = concatBytes(this._pending, bytes); this._pending = new Uint8Array(0); } const keep = options.stream ? incompleteTail(bytes) : 0; if (keep) { this._pending = bytes.slice(bytes.length - keep); bytes = bytes.subarray(0, bytes.length - keep); } return Deno.core.decode(bytes); } }
class ReadableStreamDefaultController {
  constructor(stream) { this._stream = stream; }
  get desiredSize() { const stream = this._stream; return stream._state === 'errored' ? null : stream._state === 'closed' ? 0 : stream._highWaterMark - stream._queue.length; }
  enqueue(chunk) { const stream = this._stream; if (stream._closeRequested || stream._state !== 'readable') throw new TypeError('ReadableStream is closed'); if (stream._waiters.length) stream._waiters.shift().resolve({ value: chunk, done: false }); else stream._queue.push(chunk); stream._pullIfNeeded(); }
  close() { const stream = this._stream; if (stream._closeRequested || stream._state !== 'readable') throw new TypeError('ReadableStream is closed'); stream._closeRequested = true; if (!stream._queue.length) stream._finish(); }
  error(reason) { this._stream._fail(reason); }
}
class ReadableStreamDefaultReader {
  constructor(stream) { if (stream._locked) throw new TypeError('ReadableStream is locked'); stream._locked = true; this._stream = stream; }
  read() { return this._stream ? this._stream._read() : Promise.reject(new TypeError('reader was released')); }
  cancel(reason) { return this._stream ? this._stream._cancel(reason) : Promise.reject(new TypeError('reader was released')); }
  releaseLock() { if (this._stream) { this._stream._locked = false; this._stream = null; } }
}
class ReadableStream {
  constructor(source = {}, strategy = {}) {
    Object.assign(this, { _source: source, _queue: [], _waiters: [], _state: 'readable', _closeRequested: false, _started: false, _pulling: false, _pullAgain: false, _locked: false, _highWaterMark: strategy.highWaterMark ?? 1 });
    this._controller = new ReadableStreamDefaultController(this);
    Promise.resolve(source.start?.(this._controller)).then(() => { this._started = true; this._pullIfNeeded(); }, reason => this._fail(reason));
  }
  _pullIfNeeded() {
    if (!this._started || this._state !== 'readable' || this._closeRequested || !this._source.pull || (!this._waiters.length && this._queue.length >= this._highWaterMark)) return;
    if (this._pulling) { this._pullAgain = true; return; }
    this._pulling = true;
    Promise.resolve().then(() => this._source.pull(this._controller)).then(() => { this._pulling = false; if (this._pullAgain) { this._pullAgain = false; this._pullIfNeeded(); } }, reason => this._fail(reason));
  }
  _finish() { this._state = 'closed'; for (const waiter of this._waiters.splice(0)) waiter.resolve({ value: undefined, done: true }); }
  _fail(reason) { if (this._state !== 'readable') return; this._state = 'errored'; this._error = reason; this._queue = []; for (const waiter of this._waiters.splice(0)) waiter.reject(reason); }
  _read() {
    if (this._queue.length) { const value = this._queue.shift(); if (this._closeRequested && !this._queue.length) this._finish(); else this._pullIfNeeded(); return Promise.resolve({ value, done: false }); }
    if (this._state === 'closed') return Promise.resolve({ value: undefined, done: true });
    if (this._state === 'errored') return Promise.reject(this._error);
    return new Promise((resolve, reject) => { this._waiters.push({ resolve, reject }); this._pullIfNeeded(); });
  }
  async _cancel(reason) { if (this._state === 'errored') throw this._error; if (this._state === 'closed') return; this._queue = []; this._finish(); await this._source.cancel?.(reason); }
  get locked() { return this._locked; }
  getReader() { return new ReadableStreamDefaultReader(this); }
  cancel(reason) { return this._locked ? Promise.reject(new TypeError('ReadableStream is locked')) : this._cancel(reason); }
  values() { const reader = this.getReader(); return { next: () => reader.read(), return: async value => { await reader.cancel(); reader.releaseLock(); return { value, done: true }; }, [Symbol.asyncIterator]() { return this; } }; }
  [Symbol.asyncIterator]() { return this.values(); }
}
globalThis.ReadableStream = ReadableStream; globalThis.ReadableStreamDefaultReader = ReadableStreamDefaultReader; globalThis.ReadableStreamDefaultController = ReadableStreamDefaultController;
globalThis.TextEncoder = TextEncoder; globalThis.TextDecoder = TextDecoder;
class Headers { constructor(init = []) { this._list = init instanceof Headers ? [...init] : Array.isArray(init) || init instanceof Map ? [...init].map(([k,v]) => [String(k).toLowerCase(), String(v)]) : Object.entries(init).map(([k,v]) => [k.toLowerCase(), String(v)]); } get(name) { const values=this._list.filter(([k]) => k === String(name).toLowerCase()).map(([,v])=>v); return values.length ? values.join(', ') : null; } has(name) { return this.get(name) !== null; } set(name,value) { const key=String(name).toLowerCase(); this._list=this._list.filter(([k])=>k!==key); this._list.push([key,String(value)]); } append(name,value) { this._list.push([String(name).toLowerCase(),String(value)]); } delete(name) { const key=String(name).toLowerCase(); this._list=this._list.filter(([k])=>k!==key); } entries() { return this._list[Symbol.iterator](); } keys() { return this._list.map(([k]) => k)[Symbol.iterator](); } values() { return this._list.map(([,v]) => v)[Symbol.iterator](); } forEach(callback, thisArg) { for (const [k,v] of this._list) callback.call(thisArg, v, k, this); } [Symbol.iterator]() { return this.entries(); } }
globalThis.Headers = Headers;
class Request { constructor(input, options = {}) { if (typeof input === 'object' && input.__native) { Object.assign(this,input); this.headers=new Headers(input.headers); } else { this.url=String(input); this.method=options.method||'GET'; this.headers=new Headers(options.headers); this._body=options.body??''; } } async text(){return this._body??this.body??'';} async json(){return JSON.parse(await this.text());} async formData(){return Object.fromEntries(new URLSearchParams(await this.text()));} }
globalThis.Request = Request;
class Response { constructor(body = '', options = {}) { this._rid=null; this._stream=null; if (body instanceof ReadableStream) { this._stream=body; this._body=null; } else { this._body=body == null ? '' : String(body); } this.status=options.status??200; this.statusText=options.statusText===undefined?'':String(options.statusText); if(!/^[\t\x20-\x7e\x80-\xff]*$/.test(this.statusText)) throw new TypeError(`Invalid statusText: ${JSON.stringify(this.statusText)}`); this.ok=this.status>=200&&this.status<300; this.headers=new Headers(options.headers); }
  get body() { if (this._stream === null && this._rid !== null) { const rid=this._rid; this._rid=null; this._stream=new ReadableStream({ pull: async controller => { const chunk=await Deno.core.ops.op_fetch_read(rid); if (chunk.length) controller.enqueue(chunk); else controller.close(); }, cancel: () => Deno.core.ops.op_fetch_close(rid) }, { highWaterMark: 0 }); } else if (this._stream === null) { const text=this._body; this._body=null; this._stream=new ReadableStream({ start: controller => { if (text) controller.enqueue(Deno.core.encode(text)); controller.close(); } }); } return this._stream; }
  async text() { if (this._body !== null) return this._body; if (this._rid !== null) { const rid=this._rid; this._rid=null; this._body=await Deno.core.ops.op_fetch_text(rid); return this._body; } const reader=this._stream.getReader(); let bytes=new Uint8Array(0); for (let next=await reader.read(); !next.done; next=await reader.read()) bytes=concatBytes(bytes, toBytes(next.value)); reader.releaseLock(); this._stream=null; this._body=Deno.core.decode(bytes); return this._body; }
  async bytes() { if (this._body !== null) return Deno.core.encode(this._body); const reader=this.body.getReader(); let bytes=new Uint8Array(0); for (let next=await reader.read(); !next.done; next=await reader.read()) bytes=concatBytes(bytes, toBytes(next.value)); reader.releaseLock(); return bytes; }
  async arrayBuffer() { const bytes=await this.bytes(); return bytes.buffer.slice(bytes.byteOffset, bytes.byteOffset + bytes.byteLength); }
  async json(){return JSON.parse(await this.text());} static json(value, options = {}) { const response=new Response(JSON.stringify(value),options); if(!response.headers.has('content-type')) response.headers.set('content-type','application/json'); return response; } }
globalThis.Response = Response;
globalThis.fetch = async (input, options = {}) => { const request=input instanceof Request?input:new Request(input,options); const data=await Deno.core.ops.op_fetch({url:request.url,method:request.method,headers:[...request.headers],body:request._body}); const response=new Response(null,{status:data.status,statusText:data.statusText,headers:data.headers}); response._body=null; response._rid=data.rid; return response; };
";
