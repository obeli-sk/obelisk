
Workflows orchestrate activities (and other workflows). They must be **deterministic** — given the
same inputs and event history they always produce the same sequence of calls. Obelisk records every
call and its result; on crash-recovery it replays the log, skipping already-completed calls. See
Workflows for the general model.

```toml
[[workflow_js]]
name        = "my_workflow"
location    = "${DEPLOYMENT_DIR}/workflow/my_workflow.js"
ffqn        = "myapp:demo/workflow.my-workflow"
params      = []
return_type = "result<string, string>"
```

Use `obelisk.sleep` for delays instead of `setTimeout`. `Date.now()` and `Math.random()` are safe to
use — their values are recorded in the execution log on first call and replayed deterministically on
recovery.

## `obelisk.call` — submit and await

Submits a child execution (activity or workflow) and **blocks** until it completes. Returns the ok
payload directly; throws the err payload string if the child permanently fails.

```javascript
// ffqn: myapp:demo/workflow.serial() -> result<string, string>
export default function serial() {
  let acc = 0;
  for (let i = 0; i < 10; i++) {
    obelisk.sleep({ seconds: 1 });
    const result = obelisk.call("myapp:demo/activity.step", [i, i * 200]);
    acc += Number(result);
    console.log(`step(${i})=${result}`);
  }
  return String(acc);
}
```

## `obelisk.sleep` — persistent sleep

Pauses the workflow durably — the sleep position is saved to the execution log. If the server
crashes mid-sleep and restarts, the sleep resumes where it left off.

```javascript
obelisk.sleep({ milliseconds: 300 });
obelisk.sleep({ seconds: 1 });
obelisk.sleep({ minutes: 5 });
```

## Join sets — parallel submission

Join sets let you submit multiple
child executions concurrently and await their results individually.

```javascript
// ffqn: myapp:demo/workflow.parallel() -> result<string, string>
export default function parallel() {
  const handles = [];
  for (let i = 0; i < 10; i++) {
    const js = obelisk.createJoinSet(); // optional: { name: "my-set" }
    const execId = js.submit("myapp:demo/activity.step", [i, i * 200]);
    handles.push({ i, js, execId });
  }
  let acc = 0;
  for (const { i, js, execId } of handles) {
    const response = js.joinNext(); // blocks until next result in this join set
    if (!response.ok) throw `step ${i} failed`;
    const result = obelisk.getResult(response.id); // { ok: value } or { err: value }
    acc = 10 * acc + Number(result.ok);
    obelisk.sleep({ milliseconds: 300 });
  }
  return String(acc);
}
```

**Join set API:** | Call | Returns | Description | |------|---------|-------------| |
`let js = obelisk.createJoinSet()` | join set object | Create a new join set (optionally
`{ name: "…" }`) | | `js.submit(ffqn, argArray)` | childExecId (string) | Submit a child execution
without blocking | | `js.submitDelay(duration)` | `delayId` (string) | Submit a timer (e.g.
`{ milliseconds: 500 }`). Duration key is one of: milliseconds/seconds/minutes/hours/days | |
`js.joinNext()` | `{ ok: bool, id: childExecId / delayId, type: "execution"/"delay" }` | Block until
the next result in this join set | | `js.joinNextTry()` |
`{ ok: bool, id: childExecId / delayId, type: "execution"/"delay" }` or
`{ status: "allProcessed"/"pending" }` | Attempt to get next response without waiting if no response
arrived yet | | `js.close()` | | Cancel activities and delays, await child workflows | |
`obelisk.getResult(childExecId)` | `{ ok/err: value }` | Fetch a result after child execution was
submitted and consumed with `joinNext*` |

## Random values and time

`Math.random()` and `Date.now()` are safe to use in workflow code. Their values are recorded in the
execution log on first execution and replayed identically on crash-recovery, preserving determinism.

```javascript
const rand = Math.random(); // deterministic on replay
const now = Date.now(); // deterministic on replay
```

Obelisk also provides explicit workflow-safe helpers:

```javascript
const n = obelisk.randomU64(0, 100); // u64 in [0, 100)
const n2 = obelisk.randomU64Inclusive(1, 6); // u64 in [1, 6]
const s = obelisk.randomString(8, 16); // alphanumeric, length in [8, 16)
```

## `obelisk.schedule` — fire-and-forget submission

Schedules a new top-level execution without blocking the workflow. Returns immediately; the
scheduled execution runs independently.

```javascript
const execId = obelisk.executionIdGenerate();
obelisk.schedule(execId, "myapp:demo/activity.send-email", ["user@example.com"]);
// optional schedule-at: obelisk.schedule(execId, ffqn, args, { seconds: 60 });
```

## `obelisk.stub` — inject a result for a stub activity

Stub activities have no implementation —
the result is supplied externally (via the CLI, Web UI, or `obelisk.stub`). This enables
human-in-the-loop workflows and test scenarios where a child result is provided without executing
real code.

```javascript
const js = obelisk.createJoinSet();
const execId = js.submit("myapp:stubs/approval.approve", [requestId]);
obelisk.stub(execId, { ok: "approved" }); // inject result (idempotent for same value)
js.joinNext();
const result = obelisk.getResult(execId).ok; // "approved"
```
