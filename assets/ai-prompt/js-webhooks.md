
Webhook endpoints serve HTTP requests. The handler receives a `Request` object and must
return a `Response`. Webhooks can call activities and workflows via `obelisk.call` and make
outbound HTTP requests via `fetch`. See
Webhook Endpoints for the
general model.

```toml
[[webhook_endpoint_js]]
name     = "my_webhook"
location = "${DEPLOYMENT_DIR}/webhook/my_webhook.js"
routes   = [
    { methods = ["GET"],  route = "/items" },
    { methods = ["POST"], route = "/items/:id" },
]
env_vars = [{ key = "DB_URL", value = "..." }]
[[webhook_endpoint_js.allowed_host]]
pattern = "https://api.example.com"
methods = ["GET"]
[webhook_endpoint_js.allowed_host.secrets]
env_vars = ["API_KEY"]
replace_in = ["headers"]
```

Route path segments starting with `:` are captured and exposed as `process.env` entries
inside the handler — `process.env['segment-name']` returns the captured value as a string.

## Handler signature

```javascript
export default function handle(request) {
    // sync or async
    return new Response("body", { status: 200, headers: { "content-type": "text/plain" } });
}
```

## Request object

| Property / method | Description |
|-------------------|-------------|
| `request.url` | Full request URL as a string |
| `request.method` | HTTP method (`"GET"`, `"POST"`, …) |
| `request.headers` | `Headers` object |
| `request.headers.get(name)` | Returns header value or `null`; multiple values joined by `", "` |
| `await request.text()` | Read the request body as a string |
| `await request.json()` | Parse the request body as JSON |
| `await request.formData()` | Parse the request body as form data |

### Reading the request body

```javascript
export default async function handle(request) {
    // As plain text
    const text = await request.text();

    // As JSON
    const data = await request.json();

    // As form data (application/x-www-form-urlencoded or multipart/form-data)
    const form = await request.formData();
    const field = form.get("field-name");

    return Response.json({ received: data });
}
```

## Response construction

```javascript
// Text response
return new Response("Hello!", { status: 200, headers: { "content-type": "text/plain" } });

// JSON response (sets content-type: application/json automatically)
return Response.json({ key: "value" });
return Response.json([1, 2, 3], { status: 201 });

// Proxy a fetch response directly
const resp = await fetch("https://api.example.com/data");
return resp;
```

## Calling activities and workflows

Webhooks have access to `obelisk.call` — the call blocks until the child execution
completes, so the HTTP response is only sent after the workflow finishes.

```javascript
// ffqn: myapp:demo/webhook.handle
export default function handle(request) {
    const url = new URL(request.url);
    if (url.pathname === "/serial") {
        const result = obelisk.call("myapp:demo/workflow.serial", []);
        return new Response(`serial completed: ${result}`, { status: 200 });
    }
    return new Response("not found", { status: 404 });
}
```

## Path parameters

When a route contains named segments (`:name`), the captured values are available as
`process.env` entries inside the handler:

```toml
[[webhook_endpoint_js]]
name    = "my_webhook"
location = "${DEPLOYMENT_DIR}/webhook/my_webhook.js"
routes  = ["/users/:user-id/items/:item-id"]
```

```javascript
export default function handle(request) {
    const userId = process.env['user-id'];
    const itemId = process.env['item-id'];
    return Response.json({ userId, itemId });
}
```

## Scheduling executions

Webhooks can schedule top-level executions without blocking the HTTP response. First generate
an execution ID, then pass it to `obelisk.schedule`:

```javascript
export default function handle(request) {
    const execId = obelisk.executionIdGenerate();
    obelisk.schedule(execId, "myapp:demo/activity.send-email", ["user@example.com"]);
    // optional schedule-at: obelisk.schedule(execId, ffqn, args, { seconds: 60 });
    return new Response(execId, { status: 202 });
}
```

Check or retrieve a previously scheduled or running execution:

```javascript
const status = obelisk.getStatus(execId);
// e.g. { status: "pendingAt"|"locked"|"paused"|"blockedByJoinSet" } or { status: "finished", finishedStatus:"ok"|"err"|"executionFailure" }

const result = obelisk.tryGet(execId);
// { pending: true } if not finished yet
// { ok: value } or { err: value } when done
```

Get the execution ID of the current webhook invocation:

```javascript
const myExecId = obelisk.executionIdCurrent();
// Returns the execution ID string for this webhook handler invocation
```

## Webhook API

| Call | Returns | Description |
|------|---------|-------------|
| `obelisk.call(ffqn, argArray)` | returns ok value, throws error value |
| `obelisk.executionIdGenerate()` | `executionId` (string) | Generate a unique execution ID |
| `obelisk.executionIdCurrent()` | `executionId` (string) | Get the execution ID of the current webhook invocation |
| `obelisk.schedule(execId, ffqn, argArray)` | — | Schedule a top-level execution immediately |
| `obelisk.schedule(execId, ffqn, argArray, delay)` | — | Schedule at a delay (e.g. `{ seconds: 60 }`) |
| `obelisk.getStatus(execId)` | `{ status: "pendingAt"/"locked"/"paused"/"blockedByJoinSet"/"finished", finishedStatus:"ok"/"err"/"executionFailure" }` |
| `obelisk.get(execId)`    | `{ ok/err: value }` | Blocking: waits until execution finishes |
| `obelisk.tryGet(execId)` | `{ pending: true }` or `{ ok/err: value }` | Non-blocking result fetch |

## Available globals

Same as JS activities:
`process.env`, `fetch`, `crypto.subtle`, `console`, `TextEncoder`/`TextDecoder`.

## Fetch example

```javascript
export default async function handle(request) {
    const someHeader = request.headers.get("x-custom");
    const api_key = process.env["API_KEY"]; // JS code only gets a short-lived token, transformed on outbound HTTP request.
    // Requires [[webhook_endpoint_js.allowed_host]] for the target
    const resp = await fetch("https://api.example.com/data", {
        headers: { "accept": "application/json", "authorization": `Bearer ${api_key}` }
    });
    return resp;
}
```
