
Practical patterns for building JS components — activities, workflows, and webhooks. For API
reference see JS Activities,
JS Workflows, and
JS Webhooks. For WIT types and JSON encoding see
WIT reference.

## Ready made activities

Repository [obeli-sk/components](https://github.com/obeli-sk/components/) contains ready-made
components that are published to Docker Hub. They can be added to a `deployment.toml` either by
copying `obelisk-oci.toml` from their respective folder or by using `obelisk component add`
CLI.

## Activities: Variant errors — permanent vs retryable

An activity's `return_type` can use a `variant` for the error arm instead of a plain `string`. Any
variant case whose name **contains the word `permanent`** is treated as a non-retryable failure —
Obelisk finishes the execution immediately without consuming retries. All other cases are retried up
to `max_retries`. The `execution-failed` case is reserved by Obelisk for trap/timeout escalation and
must always be present.

```toml
[[activity_js]]
name = "fetch_dev_deps"
location = "${DEPLOYMENT_DIR}/activity/fetch_dev_deps.js"
ffqn = "myorg:monitor/repos.fetch-dev-deps" # Every function must have Function Fully Qualified Name
params = [{ name = "repo", type = "string" }]
return_type = "result<string, variant { permanent-not-found, transient-error(string), execution-failed }>"
[[activity_js.allowed_host]]
pattern = "https://raw.githubusercontent.com"
methods = ["GET"]
```

Throw a snake_case string to select a no-payload case; throw `{ case_name: payload }` for a case
with payload:

```javascript
// activity/fetch_dev_deps.js
export default async function fetch_dev_deps(repo) {
  const branches = ["main", "master"];
  for (const branch of branches) {
    const url = `https://raw.githubusercontent.com/myorg/${repo}/${branch}/dev-deps.txt`;
    const resp = await fetch(url, { headers: { "user-agent": "my-monitor" } });
    if (resp.ok) return await resp.text();
    if (resp.status !== 404) {
      const msg = `HTTP ${resp.status} fetching ${url}`;
      throw { transient_error: msg }; // retried; payload = diagnostic string
    }
  }
  throw "permanent_not_found"; // not retried; no payload
}
```

A workflow calling this activity receives the variant as a thrown error. `obelisk.call` throws the
err payload; `obelisk.getResult` returns `{ err: "permanent_not_found" }` or
`{ err: { transient_error: "HTTP 503 …" } }`.

## Workflows: Parallel fan-out with named join sets

Create **one named join set per item** so the execution log labels each child by its input. Submit
all children, then drain in submission order via `joinNext()`.

```javascript
// workflow/monitor.js
export default function run() {
  const repos = obelisk.call("myorg:monitor/repos.list-repos", []);

  // Submit one fetch per repo.  Named join sets make the event log
  // self-describing (the WebUI shows "join-set: my-repo" instead of an
  // opaque generated id).  Join set names allow only [A-Za-z0-9\-\/].
  const perRepo = [];
  for (const repo of repos) {
    const js = obelisk.createJoinSet({ name: sanitizeJoinSetName(repo) });
    js.submit("myorg:monitor/repos.fetch-dev-deps", [repo]);
    perRepo.push({ repo, js });
  }

  // `joinNext` drains the join set - ordered based on responses.
  const versions = [];
  for (const { repo, js } of perRepo) {
    const response = js.joinNext();
    if (!response.ok) {
      console.warn("fetch failed for", repo);
      continue;
    }
    const result = obelisk.getResult(response.id);
    // result is { ok: <string> } | { err: <variant> }
    if (result && "ok" in result) {
      const version = parseVersion(result.ok);
      if (version !== null) versions.push([repo, version]);
    }
  }

  versions.sort((a, b) => a[0].localeCompare(b[0]));
  return versions;
}

function sanitizeJoinSetName(s) {
  return s.replace(/[^A-Za-z0-9\-\/]/g, "-");
}
```

**Join set name constraints:**

- Allowed characters: `A-Z`, `a-z`, `0-9`, `-`, `/`
- Must be unique within the workflow execution (reuse → `conflict` error)
- Omit `{ name }` to get an auto-generated name

For simpler cases where every child maps to an execution ID, index results via the returned ID:

```javascript
const js = obelisk.createJoinSet();
const idToInput = {};
for (const item of items) {
  const execId = js.submit("ns:pkg/iface.process", [item]);
  idToInput[execId] = item;
}
for (let i = 0; i < items.length; i++) {
  const response = js.joinNext();
  if (!response.ok) continue;
  const result = obelisk.getResult(response.id);
  const item = idToInput[response.id];
  // use result.ok / result.err
}
```

## Workflows: Saga pattern — crash-safe compensation

Wrap the main action in `try/catch`, capture any error, run the compensation unconditionally, then
re-throw. The compensation always runs because Obelisk replays the execution log on restart and
continues from the last completed step.

Keep the outer saga workflow **minimal** — move complex logic into a child workflow so a bug in
complex logic fails the child (catchable), not the outer saga itself:

```javascript
// workflow/run.js — outer saga; kept intentionally simple
export default function run(app_name, org_slug, prompt) {
  let result = null,
    error = null;

  // Execute inner workflow; capture failure so cleanup still runs.
  try {
    result = obelisk.call("demo:fly-agent/workflow.agent", [app_name, org_slug, prompt]);
  } catch (e) {
    error = String(e);
    console.log(`Agent workflow failed: ${error}`);
  }

  // Compensation: always delete the app — even if the server crashed between
  // the try/catch above and here, Obelisk replays from this point on restart.
  try {
    obelisk.call("obelisk-flyio:activity-fly-http/apps@1.0.0-beta.delete", [app_name, true]);
  } catch (e) {
    console.log(`Cleanup failed (manual action may be needed): ${e}`);
  }

  if (error !== null) throw error;
  return result;
}
```

The `try/catch` branch taken is recorded in the execution log — on replay, Obelisk always follows
the same branch.

## Workflows Polling external state

Use a bounded `for` loop with `obelisk.sleep` between probes. The sleep position is durable: a
server crash during the loop resumes from the last completed step on restart.

```javascript
// workflow/agent.js — inner workflow
export default function agent(app_name, org_slug, prompt) {
  // ... create VM ...

  // Poll until the VM is in the 'started' state.
  let started = false;
  for (let i = 0; i < 20; i++) {
    const machine = obelisk.call("obelisk-flyio:activity-fly-http/machines@1.0.0-beta.get", [
      app_name,
      machine_id,
    ]);
    if (machine !== null && machine.state === "started") {
      started = true;
      break;
    }
    console.log(`VM state: ${machine ? machine.state : "unknown"}, retrying in 3s`);
    obelisk.sleep({ seconds: 3 });
  }
  if (!started) throw "VM did not reach 'started' state within timeout";

  // Poll until the result file appears.
  let output = null;
  for (let i = 0; i < 30; i++) {
    const cat = obelisk.call("obelisk-flyio:activity-fly-http/machines@1.0.0-beta.exec", [
      app_name,
      machine_id,
      ["cat", "/result.txt"],
      { timeout_secs: 10, stdin: null },
    ]);
    if (cat.exit_code === 0) {
      output = (cat.stdout || "").trim();
      break;
    }
    console.log(`Result not ready yet (attempt ${i + 1}/30)`);
    obelisk.sleep({ seconds: 5 });
  }
  if (output === null) throw "Agent did not produce a result within timeout";
  return output;
}
```

Keep the activity probe **cheap and idempotent**. Always set a hard loop ceiling and an explicit
timeout error so the workflow terminates rather than looping forever.

## Webhooks: Querying past executions

A webhook can query the Obelisk Web API directly via `fetch` to render the result of a previously
completed workflow run. The API is on `:5005` by default; inject the URL via `env_vars` so it is
configurable:

```toml
[[webhook_endpoint_js]]
name = "show"
location = "${DEPLOYMENT_DIR}/webhook/show.js"
routes = [{ methods = ["GET"], route = "/" }]
env_vars = [{ key = "OBELISK_API_URL", value = "${OBELISK_API_URL:-http://127.0.0.1:5005}" }]
[[webhook_endpoint_js.allowed_host]]
pattern = "${OBELISK_API_URL:-http://127.0.0.1:5005}"
methods = ["GET"]
```

```javascript
// webhook/show.js
const WORKFLOW_FFQN = "myorg:monitor/monitor.run";

export default async function handle(_request) {
  const apiBase = process.env["OBELISK_API_URL"];

  // List recent executions, newest first (default).  Find the latest finished one.
  const listResp = await fetch(
    `${apiBase}/v1/executions?ffqn_prefix=${encodeURIComponent(WORKFLOW_FFQN)}&length=50`,
    { headers: { accept: "application/json" } },
  );
  if (!listResp.ok) {
    return new Response(`Failed to list executions: HTTP ${listResp.status}`, { status: 502 });
  }
  const executions = await listResp.json();
  const finished = executions.find((e) => e.pending_state?.status === "finished");
  if (!finished) {
    return new Response("No finished execution found.", { status: 200 });
  }

  // Fetch the return value.
  const retResp = await fetch(
    `${apiBase}/v1/executions/${encodeURIComponent(finished.execution_id)}`,
    { headers: { accept: "application/json" } },
  );
  if (!retResp.ok) {
    return new Response(`Failed to fetch result: HTTP ${retResp.status}`, { status: 502 });
  }
  // RetVal shape: { ok: <value> } | { err: <value> } | { execution_error: ... }
  const retVal = await retResp.json();
  if ("ok" in retVal) {
    return Response.json(retVal.ok);
  } else if ("err" in retVal) {
    return new Response(`Workflow error: ${JSON.stringify(retVal.err)}`, { status: 200 });
  } else {
    return new Response(`Execution failure: ${JSON.stringify(retVal.execution_error)}`, {
      status: 200,
    });
  }
}
```

`pending_state.status` values: `locked`, `pending_at`, `blocked_by_join_set`, `paused`, `finished`.
Pass `?follow=true` to `GET /v1/executions/{id}` instead of `425 Too Early` while still running. See
Programmatic access for the full API.

## Workflow `obelisk.*` API

| Call                                                     | Returns                              | Description                                                                         |
| -------------------------------------------------------- | ------------------------------------ | ----------------------------------------------------------------------------------- |
| `obelisk.call(ffqn, argArray)`                           | returns ok value, throws error value |
| `obelisk.executionIdGenerate()`                          | `executionId` (string)               | Generate a unique execution ID                                                      |
| `obelisk.schedule(execId, ffqn, argArray)`               | `executionId` (string)               | Schedule a top-level execution immediately                                          |
| `obelisk.schedule(execId, ffqn, [arg1], { seconds: 1 })` | `executionId` (string)               | Delay is one of: milliseconds/seconds/minutes/hours/days                            |
| `obelisk.sleep(duration)`                                | time when execution became pending   | Persistent blocking sleep, duration: {milliseconds/seconds/minutes/hours/days: int} |
| `obelisk.stub(execId, result)`                           |                                      | Supplies a result to a stub activity execution                                      |
| `obelisk.randomU64(min, max)`                            | u64                                  |                                                                                     |
| `obelisk.randomString(minLen, maxLen)`                   | string                               |                                                                                     |

## Workflow join set API

| Call                               | Returns                                                                                                        | Description                                                                      |
| ---------------------------------- | -------------------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------- |
| `let js = obelisk.createJoinSet()` | join set object                                                                                                | Create a new join set (optionally `{ name: "…" }`)                               |
| `js.submit(ffqn, argArray)`        | childExecId (string)                                                                                           | Submit a child execution without blocking                                        |
| `js.submitDelay(duration)`         | `delayId` (string)                                                                                             | Submit a timer, duration: {milliseconds/seconds/minutes/hours/days: int}         |
| `js.joinNext()`                    | `{ ok: bool, id: childExecId / delayId, type: "execution"/"delay" }`                                           | Block until the next result in this join set                                     |
| `js.joinNextTry()`                 | `{ ok: bool, id: childExecId / delayId, type: "execution"/"delay" }` or `{ status: "allProcessed"/"pending" }` | Attempt to get next response without waiting if no response arrived yet          |
| `js.close()`                       |                                                                                                                | Cancel activities and delays, await child workflows                              |
| `obelisk.getResult(childExecId)`   | `{ ok/err: value }`                                                                                            | Fetch a result after child execution was submitted and consumed with `joinNext*` |

## Webhook `obelisk.*` API

| Call                                                       | Returns                                                                                                                 | Description                                              |
| ---------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------- | -------------------------------------------------------- |
| `obelisk.call(ffqn, argArray)`                             | returns ok value, throws error value                                                                                    |
| `obelisk.executionIdGenerate()`                            | `executionId` (string)                                                                                                  | Generate a unique execution ID                           |
| `obelisk.executionIdCurrent()`                             | `executionId` (string)                                                                                                  | Get the execution ID of the current webhook invocation   |
| `obelisk.schedule(execId, ffqn, argArray)`                 | `executionId` (string)                                                                                                  | Schedule a top-level execution immediately               |
| `obelisk.schedule(execId, ffqn, argArray, { seconds: 1 })` | `executionId` (string)                                                                                                  | Delay is one of: milliseconds/seconds/minutes/hours/days |
| `obelisk.getStatus(execId)`                                | `{ status: "pendingAt"/"locked"/"paused"/"blockedByJoinSet"/"finished", finishedStatus:"ok"/"err"/"executionFailure" }` |
| `obelisk.get(execId)`                                      | `{ ok/err: value }`                                                                                                     | Blocking: waits until execution finishes                 |
| `obelisk.tryGet(execId)`                                   | `{ pending: true }` or `{ ok/err: value }`                                                                              | Non-blocking result fetch                                |

Webhooks also have `fetch`, `Response`, `Request`, `process.env`, `crypto.subtle` — same as
JS Activities.

## Structured concurrency on join set close

When a join set is closed (via `js.close()` or implicitly when the workflow returns or throws):

- Unpolled child **activities** and **delays** are **cancelled**.
- Unpolled child **workflows** are **awaited** (not cancelled).

`obelisk.call` creates and closes a join set internally — the same rules apply. If you submit to a
join set and return without draining it, any pending activities are cancelled but pending
sub-workflows continue running. To wait for them, drain the join set before returning.

## Running an Obelisk application

### Verify before starting

Check that the deployment configuration is valid — compiles all JS components and resolves all types
— without requiring real credentials:

```sh
obelisk server verify --ignore-missing-env-vars -d deployment.toml
```

### Start the server

```sh
obelisk server run --deployment deployment.toml
```

After the first run the deployment is persisted to the database; subsequent restarts (e.g. after a
crash) can omit `--deployment`:

```sh
obelisk server run
```

### Submit a one-off execution and follow it

```sh
obelisk execution submit my-namespace:my-pkg/my-iface.my-fn '["arg1", 42]'
# prints: E_01KN...
```

Execution IDs are `E_`-prefixed ULIDs (e.g. `E_01KN2X...`).

Block until finished and print the result:

```sh
obelisk execution submit --follow my-namespace:my-pkg/my-iface.my-fn '[]'
```

Or follow a previously submitted execution by ID:

```sh
obelisk execution get --follow E_01KN2X...
## Common pitfalls

- **Workflows cannot use `fetch`.** Push all I/O (HTTP, file, database) into activities.
- **`max_retries=0` does not mean idempotency is optional.** A server restart before an activity
  finishes will retry it regardless. Use `permanent-*` variant cases to disable retry on specific
  errors without losing retries for transient ones.
- **`Math.random()` and `Date.now()` are deterministic in workflows.** Their values are recorded
  in the execution log on first call and replayed identically on crash-recovery.
- **Join set names must be unique per execution.** Derive them from stable inputs (IDs, keys)
  and sanitize to `[A-Za-z0-9\-\/]`.
- **`425 Too Early`** from `GET /v1/executions/{id}` means the execution is still running; use
  `?follow=true` to long-poll, or poll `getStatus` / `tryGet` from the webhook side.
- **Component and cron names must use only `[a-zA-Z0-9_]`** — hyphens are not allowed. Use
  underscores instead (e.g. `daily_monitor`, not `daily-monitor`).
- **Secret env vars must not duplicate a key from the component's `env_vars`.**
  A variable declared in `[allowed_host.secrets] env_vars` must not also appear in the
  component-level `env_vars`; the runtime injects it automatically and will reject the
  configuration with a "collides with an `env_vars` entry" error otherwise.
```
