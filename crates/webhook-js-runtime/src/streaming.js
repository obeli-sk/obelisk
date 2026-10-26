// Streamed bodies for Boa webhooks. Called with the native helpers, returns `serve`.
(native) => {
  const encoder = new TextEncoder();
  const decoder = new TextDecoder();
  const toBytes = (value) => {
    if (typeof value === "string") return encoder.encode(value);
    if (value instanceof Uint8Array) return value;
    if (ArrayBuffer.isView(value)) return new Uint8Array(value.buffer, value.byteOffset, value.byteLength);
    if (value instanceof ArrayBuffer) return new Uint8Array(value);
    throw new TypeError("body chunk must be a string, an ArrayBuffer or an ArrayBufferView");
  };

  // Minimal ReadableStream: start, pull, cancel, highWaterMark, readers and async iteration.
  class ReadableStreamDefaultController {
    constructor(stream) { this._stream = stream; }
    get desiredSize() {
      const stream = this._stream;
      if (stream._state === "errored") return null;
      return stream._state === "closed" ? 0 : stream._highWaterMark - stream._queue.length;
    }
    enqueue(chunk) {
      const stream = this._stream;
      if (stream._closeRequested || stream._state !== "readable") throw new TypeError("ReadableStream is closed");
      if (stream._waiters.length) stream._waiters.shift().resolve({ value: chunk, done: false });
      else stream._queue.push(chunk);
      stream._pullIfNeeded();
    }
    close() {
      const stream = this._stream;
      if (stream._closeRequested || stream._state !== "readable") throw new TypeError("ReadableStream is closed");
      stream._closeRequested = true;
      if (!stream._queue.length) stream._finish();
    }
    error(reason) { this._stream._fail(reason); }
  }

  class ReadableStreamDefaultReader {
    constructor(stream) {
      if (stream._locked) throw new TypeError("ReadableStream is locked");
      stream._locked = true;
      this._stream = stream;
    }
    read() { return this._stream ? this._stream._read() : Promise.reject(new TypeError("reader was released")); }
    cancel(reason) { return this._stream ? this._stream._cancel(reason) : Promise.reject(new TypeError("reader was released")); }
    releaseLock() {
      if (this._stream) {
        this._stream._locked = false;
        this._stream = null;
      }
    }
  }

  class ReadableStream {
    constructor(source = {}, strategy = {}) {
      Object.assign(this, {
        _source: source, _queue: [], _waiters: [], _state: "readable", _closeRequested: false,
        _started: false, _pulling: false, _pullAgain: false, _locked: false,
        _highWaterMark: strategy.highWaterMark ?? 1,
      });
      this._controller = new ReadableStreamDefaultController(this);
      Promise.resolve(source.start?.(this._controller)).then(
        () => { this._started = true; this._pullIfNeeded(); },
        (reason) => this._fail(reason),
      );
    }
    _pullIfNeeded() {
      if (!this._started || this._state !== "readable" || this._closeRequested || !this._source.pull) return;
      if (!this._waiters.length && this._queue.length >= this._highWaterMark) return;
      if (this._pulling) { this._pullAgain = true; return; }
      this._pulling = true;
      Promise.resolve().then(() => this._source.pull(this._controller)).then(
        () => {
          this._pulling = false;
          if (this._pullAgain) { this._pullAgain = false; this._pullIfNeeded(); }
        },
        (reason) => this._fail(reason),
      );
    }
    _finish() {
      this._state = "closed";
      for (const waiter of this._waiters.splice(0)) waiter.resolve({ value: undefined, done: true });
    }
    _fail(reason) {
      if (this._state !== "readable") return;
      this._state = "errored";
      this._error = reason;
      this._queue = [];
      for (const waiter of this._waiters.splice(0)) waiter.reject(reason);
    }
    _read() {
      if (this._queue.length) {
        const value = this._queue.shift();
        if (this._closeRequested && !this._queue.length) this._finish();
        else this._pullIfNeeded();
        return Promise.resolve({ value, done: false });
      }
      if (this._state === "closed") return Promise.resolve({ value: undefined, done: true });
      if (this._state === "errored") return Promise.reject(this._error);
      return new Promise((resolve, reject) => { this._waiters.push({ resolve, reject }); this._pullIfNeeded(); });
    }
    async _cancel(reason) {
      if (this._state === "errored") throw this._error;
      if (this._state === "closed") return;
      this._queue = [];
      this._finish();
      await this._source.cancel?.(reason);
    }
    get locked() { return this._locked; }
    getReader() { return new ReadableStreamDefaultReader(this); }
    cancel(reason) { return this._locked ? Promise.reject(new TypeError("ReadableStream is locked")) : this._cancel(reason); }
    values() {
      const reader = this.getReader();
      return {
        next: () => reader.read(),
        return: async (value) => { await reader.cancel(); reader.releaseLock(); return { value, done: true }; },
        [Symbol.asyncIterator]() { return this; },
      };
    }
    [Symbol.asyncIterator]() { return this.values(); }
  }
  globalThis.ReadableStream = ReadableStream;
  globalThis.ReadableStreamDefaultReader = ReadableStreamDefaultReader;
  globalThis.ReadableStreamDefaultController = ReadableStreamDefaultController;

  // The native Response keeps headers and status; a streamed body lives beside it.
  const STREAM = Symbol("obelisk.bodyStream");
  const RID = Symbol("obelisk.fetchBody");
  const hide = (object, key, value) =>
    Object.defineProperty(object, key, { value, writable: true, configurable: true, enumerable: false });
  const NativeResponse = globalThis.Response;
  const proto = NativeResponse.prototype;
  const nativeBytes = proto.bytes;
  const nativeText = proto.text;
  const nativeJson = proto.json;
  const nativeClone = proto.clone;
  const isStreamed = (response) => response[STREAM] !== undefined || response[RID] !== undefined;

  function Response(body, init) {
    if (new.target === undefined) throw new TypeError("calling a builtin Response constructor without new is forbidden");
    if (body instanceof ReadableStream) {
      const response = new NativeResponse(null, init);
      hide(response, STREAM, body);
      return response;
    }
    return new NativeResponse(body, init);
  }
  Response.prototype = proto;
  Response.json = (data, init) => NativeResponse.json(data, init);
  Response.error = () => NativeResponse.error();
  Response.redirect = (url, status) => NativeResponse.redirect(url, status);
  hide(proto, "constructor", Response);
  globalThis.Response = Response;

  const bodyStream = (response) => {
    if (response[STREAM] !== undefined) return response[STREAM];
    const rid = response[RID];
    let stream;
    if (rid !== undefined) {
      hide(response, RID, undefined);
      stream = new ReadableStream({
        pull: async (controller) => {
          const chunk = await native.fetchRead(rid);
          if (chunk === undefined) controller.close();
          else controller.enqueue(chunk);
        },
        cancel: () => native.fetchClose(rid),
      }, { highWaterMark: 0 });
    } else {
      stream = new ReadableStream({
        start: async (controller) => {
          const bytes = await nativeBytes.call(response);
          if (bytes.length) controller.enqueue(bytes);
          controller.close();
        },
      });
    }
    hide(response, STREAM, stream);
    return stream;
  };

  // Read a streamed body to the end, or return undefined for a buffered one.
  const collect = async (response, asText) => {
    const rid = response[RID];
    if (rid !== undefined) {
      hide(response, RID, undefined);
      return native.fetchCollect(rid, asText);
    }
    const stream = response[STREAM];
    if (stream === undefined) return undefined;
    const reader = stream.getReader();
    const parts = [];
    let length = 0;
    for (let next = await reader.read(); !next.done; next = await reader.read()) {
      const bytes = toBytes(next.value);
      parts.push(bytes);
      length += bytes.length;
    }
    reader.releaseLock();
    const bytes = new Uint8Array(length);
    let offset = 0;
    for (const part of parts) {
      bytes.set(part, offset);
      offset += part.length;
    }
    return asText ? decoder.decode(bytes) : bytes;
  };

  Object.defineProperty(proto, "body", { get() { return bodyStream(this); }, configurable: true });
  hide(proto, "bytes", async function () { return (await collect(this, false)) ?? nativeBytes.call(this); });
  hide(proto, "arrayBuffer", async function () {
    const bytes = await this.bytes();
    return bytes.buffer.slice(bytes.byteOffset, bytes.byteOffset + bytes.byteLength);
  });
  hide(proto, "text", async function () { return (await collect(this, true)) ?? nativeText.call(this); });
  hide(proto, "json", async function () {
    const text = await collect(this, true);
    return text === undefined ? nativeJson.call(this) : JSON.parse(text);
  });
  hide(proto, "clone", function () {
    if (isStreamed(this)) throw new TypeError("cloning a streamed Response is not supported");
    return nativeClone.call(this);
  });

  globalThis.fetch = async (input, init) => {
    const signal = init?.signal;
    if (signal?.aborted) throw signal.reason;
    const [response, rid] = await native.fetch(new Request(input, init));
    hide(response, RID, rid);
    return response;
  };

  return async (handler, request) => {
    const response = await handler(request);
    if (!(response instanceof NativeResponse)) {
      throw new TypeError("handler must return a Response (e.g. `new Response(...)` or `Response.json(...)`)");
    }
    if (!isStreamed(response)) return response;
    const reader = bodyStream(response).getReader();
    native.respond(response);
    try {
      for (let next = await reader.read(); !next.done; next = await reader.read()) {
        const bytes = toBytes(next.value);
        if (bytes.length && !(await native.write(bytes))) {
          await reader.cancel();
          break;
        }
      }
    } catch (err) {
      await native.abort(String(err));
      throw err;
    }
    native.close();
    return undefined;
  };
}
