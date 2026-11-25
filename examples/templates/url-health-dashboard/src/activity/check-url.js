// A network error throws, so Obelisk retries the check before the workflow sees the error.
export default async function checkUrl(url) {
  const started = Date.now();
  const response = await fetch(url, { redirect: "follow" });
  const body = await response.text();
  const title = body.match(/<title[^>]*>([\s\S]*?)<\/title>/i);
  return {
    status: response.status,
    status_text: response.statusText,
    latency_ms: Date.now() - started,
    size_bytes: body.length,
    content_type: response.headers.get("content-type"),
    server: response.headers.get("server"),
    title: title ? title[1].replace(/\s+/g, " ").trim().slice(0, 200) : null,
  };
}
