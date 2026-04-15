
Activities are the units of side-effectful work. They run inside a WASM sandbox, are retried
automatically on failure or timeout, and must be **idempotent** — Obelisk may execute them
more than once. See Activities
for the general model and WIT reference
for the full type and JSON encoding reference.

```toml
[[activity_js]]
name         = "my_activity"
location     = "${DEPLOYMENT_DIR}/activity/my_activity.js"
ffqn         = "myapp:demo/activity.my-activity"
params       = [{ name = "input", type = "string" }]
return_type  = "result<string>"
exec.lock_expiry.seconds = 10   # executor lease duration; if the activity runs longer,
                                # Obelisk considers it lost and reschedules it
max_retries  = 5
env_vars     = [{ key = "API_KEY", value = "secret" }]
[[activity_js.allowed_host]]
pattern = "https://api.example.com"
methods = ["GET", "POST"]
```

## Return values and errors

The JS runtime maps `return` and `throw` to the `result` return type.

### `result` — no payload on either side

```javascript
export default function fn() {
    return;       // ok variant, no payload
    // throw null // err variant, no payload — Obelisk retries the activity
}
```

### `result<string>` — ok carries a value; err has no payload

```javascript
// ffqn: myapp:demo/activity.greet(name: string) -> result<string>
export default function greet(name) {
    console.info("Greeting", name);
    return "Hello, " + name + "!";  // ok("Hello, ...")
    // throw null                   // err (no payload) — triggers retry
    // throw "oops"                 // FATAL — not allowed, causes permanent failure
}
```

For `result<T>` (no err type), `throw null` is the only valid way to produce the err variant.
Throwing any non-null value is a permanent failure — no retries.

### `result<string, string>` — both variants carry a string

```javascript
// ffqn: myapp:demo/activity.fetch-data(url: string) -> result<string, string>
export default async function fetch_data(url) {
    const resp = await fetch(url);
    if (!resp.ok) throw `HTTP ${resp.status}`;   // err("HTTP 404") — triggers retry
    return await resp.text();                     // ok(body)
    // throw new Error("msg")                     // also works; .message is used
}
```

When an activity returns the err variant, Obelisk retries it according to the retry policy.
After all retries are exhausted, the error is propagated to the caller. The err variant is
also produced by Obelisk itself — on timeout, trap (panic), or type-check failure — even
when the JS code never throws.

## Available globals

| Global | Description |
|--------|-------------|
| `process.env` | Access environment variables configured via `env_vars` |
| `fetch(url, options)` | Outbound HTTP; requires `[[activity_js.allowed_host]]` entries |
| `crypto.subtle` | [SubtleCrypto](https://developer.mozilla.org/en-US/docs/Web/API/SubtleCrypto) (HMAC, AES, RSA, …) |
| `setTimeout` / `Promise` | Async sleep via `await new Promise(r => setTimeout(r, ms))` |
| `console.log/info/warn/error` | Structured log entries stored in the execution log |
| `TextEncoder` / `TextDecoder` | UTF-8 encoding |

## Examples

**Environment variable:**
```javascript
export default function read_env(key) {
    const value = process.env[key];
    if (value === undefined) throw "env var not found: " + key;
    return value;
}
```

**Async sleep:**
```javascript
export default async function sleep(milliseconds) {
    await new Promise(r => setTimeout(r, milliseconds));
}
```

**Outbound HTTP with request headers:**
```javascript
// params: url: string, headers: list<tuple<string, string>>
export default async function fetch_get(url, headersArr) {
    const resp = await fetch(url, { headers: Object.fromEntries(headersArr) });
    return await resp.text();
}
```

**HMAC-SHA256 with crypto.subtle:**
```javascript
export default async function hmac_sign(key_str, message) {
    const enc = new TextEncoder();
    const key = await crypto.subtle.importKey(
        "raw", enc.encode(key_str),
        { name: "HMAC", hash: "SHA-256" }, false, ["sign"]
    );
    const sig = await crypto.subtle.sign("HMAC", key, enc.encode(message));
    return [...new Uint8Array(sig)].map(b => b.toString(16).padStart(2, "0")).join("");
}
```

**Record return type:**
```javascript
// return_type = "result<record { name: string, count: u32 }>"
export default function make_record(name) {
    return { name: name, count: 42 };  // returned as {"name": "...", "count": 42}
}
```
