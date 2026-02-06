# Ad-hoc JavaScript Workflow

A WASM component that executes JavaScript code as an Obelisk workflow.

## Overview

This component embeds the [Boa](https://boajs.dev/) JavaScript engine and exposes
Obelisk workflow support functions to JavaScript code. It's designed for ad-hoc
workflows submitted dynamically via API/CLI.

## Building

```bash
cargo build -p adhoc-js-workflow --target wasm32-wasip2 --release
```

Output: `target/wasm32-wasip2/release/adhoc_js_workflow.wasm` (~4.2MB)

## WIT Interface

```wit
export execute: func(js-code: string, params: string) -> result<string, string>;
```

## JavaScript API

### Entry Point

```javascript
function main(params) {
  // params is JSON-parsed from the second argument
  // Return value is JSON-serialized as workflow result
  return { result: "computed value" };
}
```

### Global `obelisk` Object

```javascript
// Join sets
obelisk.createJoinSet()                    // -> JoinSet
obelisk.createJoinSet({ name: "my-name" }) // -> JoinSet (named)

// Sleep (synchronous, blocks workflow)
obelisk.sleep({ seconds: 10 })             // -> { seconds, nanoseconds }
obelisk.sleep({ milliseconds: 500 })
obelisk.sleep({ minutes: 5 })
obelisk.sleep({ at: { seconds, nanoseconds } })

// Random (deterministic on replay)
obelisk.randomU64(min, maxExclusive)       // -> number
obelisk.randomU64Inclusive(min, max)       // -> number
obelisk.randomString(minLen, maxLenExcl)   // -> string

// Get result of awaited execution
obelisk.getResult(executionId)             // -> { ok: value } | { err: value }
```

### JoinSet Object

```javascript
const js = obelisk.createJoinSet();

// Submit child execution (returns immediately)
js.submit(
  "namespace:pkg/interface.function",  // FFQN string
  [param1, param2],                    // JSON-serializable params
  { timeout: { seconds: 30 } }         // optional config
)  // -> executionId string

// Submit delay
js.submitDelay({ seconds: 5 })         // -> delayId string

// Wait for next response (synchronous, blocks)
js.joinNext()
// -> { type: "delay", id: "...", ok: true }
// -> { type: "delay", id: "...", ok: false, error: "cancelled" }
// -> { type: "execution", id: "...", ok: true|false }
// throws when all processed

// Accessors
js.id()     // -> "g:0" | "n:my-name"
js.close()  // explicit close
```

### Console

```javascript
console.trace("verbose trace")   // -> trace level
console.debug("debug info")      // -> debug level
console.log("message", obj)      // -> info level
console.warn("warning")          // -> warn level
console.error("error")           // -> error level
```

## Example Workflows

### Fan-out/Fan-in

```javascript
function main({ urls }) {
  const js = obelisk.createJoinSet();
  
  // Fan out
  const execIds = urls.map(url => 
    js.submit("my:http/fetcher.fetch", [url])
  );
  
  // Fan in
  for (let i = 0; i < urls.length; i++) {
    js.joinNext();
  }
  
  // Collect in original order
  return execIds.map(id => obelisk.getResult(id));
}
```

### Timeout Pattern

```javascript
function main({ taskId }) {
  const js = obelisk.createJoinSet();
  
  const execId = js.submit("slow:svc/api.process", [taskId]);
  js.submitDelay({ seconds: 30 });
  
  const response = js.joinNext();
  
  if (response.type === "delay") {
    js.close();  // cancels pending execution
    throw new Error("timeout");
  }
  
  return obelisk.getResult(execId);
}
```

## Future Work

- `[[workflow-js]]` config for permanent JS workflows with first-class backtrace support
- `[[workflow-ts]]` for TypeScript workflows
