# Immediate goals

## feat: Turn `execution-id` into a resource
Make it symetrical with `join-set-id`, disallow users from `-await`ing on a string,
can track the join set created.

## feat: `execution-id.get()`, can be called multiple times

## fix: Deadlock when calling `-await-next` while nothing is in queue

## feat: Add `obelisk generate`
`obelisk generate config` blank(just webui),fibo, testing, stargazers
`obelisk generate wit -c obelisk.toml --out-dir wit/deps/ my-activity`
`obelisk generate wit` extensions for webhook (just `-schedule` ext)
`obelisk new` - show templates, blank workflow should have obelisk types and workflow support
`obelisk add --oci path` - add WIT files + extensions

## feat: Create an SDK for workflows and other (logging - maybe a proc macro)
Convert delays into Duration

## feat: Inspect remote or cached components
`obelisk client component inspect/wit` should accept: path, componentId, oci location

## feat: deps.toml tool
obelisk wit update-deps

## feat: obelisk.lock
Save the digest of each downloaded OCI image into a lockfile, then remove the optional digest from toml,

## feat: Support WIT-only WASM files
Allow pushing and declaring WASM resources only containing the WIT.
Needed for external activities.

## feat: Long running monitor trigger
Similar to a webhook endpoint, new component type with `main`, restarts on exit or trap.
Can listen to a HTTP stream and trigger an execution.
Could be used to monitor MQTT, UDP etc.

## feat: Heterogenous join sets, allowing one join set to combine multiple function signatures and delays

## feat: Expose network configuration for activities, webhooks
Enable allow/deny lists of remote hosts.

## feat: Keepalives for activities, extending the lock until completion

## fix: Track all unhandled execution errors when closing join sets
Currently only the first unhandled error is tracked, as well as only one root cause.

## fix: Rename all gRPC history event messages to match storage.rs
E.g. `JoinSetCreated` to `JoinSetCreate`.

## Future ideas
* Optional caching of activity executions with a TTL - serve cached response if parameters are the same
* Interactive CLI for execution management
* External activities gRPC API
* External executors support - starting executions solely based on WIT exports. External executors must share write access to the sqlite database.
* OpenAPI, GraphQL activity generator
* Backpressure: Limits on pending queues, or an eviction strategy, slow down on `LimitReached`
* Labels restricting workflows/activities to executors
* Periodic scheduling
* [Deadline propagation](https://sre.google/sre-book/addressing-cascading-failures)
* [Cancellation propagation](https://sre.google/sre-book/addressing-cascading-failures)
* Queue capacity setting, adding backpressure to execution submission
* Ability to simulate behaviour of the system with injected failures
* (Manual) Notify activities. When called, thir return value must be supplied via an API endpoint.
* An API for listing executions with their open notify activities.
* Read only query function that can be called during an await point or after execution finishes.
* Optional stdout,stderr persistence / forwarding
* Smart dependency routing from a caller via an interface import to one of many components that export it.
* Smart retries - Retry budget, disabling retries when the activity is failing certain % of requests
* Configurable jitter added to retries
* Workflow memory snapshots for faster replay
* Time traveling debugger for workflows, that works accross WASM deployments
* Ability to hotfix a set of workflows, with an approval system when non determinism is detected - forking the execution log
* Trace strings to their origin accross workflows and activities
* Webhook endpoint mappings: running a single function, translating between HTTP and WIT defined parameters and return value
* Distributed tracing context forwarding for outgoing HTTP as well as webhooks
* Allow specifying permanent error variants in as annotations in WIT
* Support for (distributed) sagas - define rollbacks on activities, call them on failed workflows
* Investigate code-coverage for workflow steps
