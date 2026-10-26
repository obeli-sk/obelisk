// Streamed response bodies for Boa webhooks. Called with the native helpers and the
// `boa_common::fetch_stream` helpers, returns `serve`.
(native, { toBytes, bodyStream, isStreamed, NativeResponse }) => {
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
