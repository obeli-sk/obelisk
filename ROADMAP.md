# Immediate goals

## fix: Get rid of streaming in WebUI
Listening to multiple streams will exhaust all the available connections in a browser.
Use timer instead, Add `stream` parameter.

## feat: Turn `execution-id` into a resource
Make it symetrical with `join-set-id`, disallow users from `-await`ing on a string,
can track the join set created.

## fix: Deadlock when calling `-await-next` while nothing is in queue

## feat: Add `obelisk generate`
`obelisk generate config` blank,webui,fibo
`obelisk generate wit -c obelisk.toml --out-dir wit/deps/ my-activity`
`obelisk new` - show templates

## feat: Create an SDK for workflows and other (logging - maybe a proc macro)

## feat: Versioning
versioning to tell when a component is not supported.
The generated WITs should have a version.
SDK needs to be versioned as well.

## feat: `-await(execution-id)`, can be called multiple times

## fix: Make `JoinSetId` in the format `executionid#0`
Avoid possible conflicts when creating join sets.

## refactor Remove wasmtime from parser

## feat: deps.toml tool
obelisk wit update-deps

## feat: Support WIT-only WASM files
Allow pushing and declaring WASM resources only containing the WIT.
Needed for external activities.

## feat: Long running monitor trigger
Similar to a webhook endpoint, new component type with `main`, restarts on exit or trap.
Can listen to a HTTP stream and trigger an execution.
Could be used to monitor MQTT, UDP etc.

## Heterogenous join sets, allowing one join set to combine multiple function signatures and delays

## Expose network configuration for activities, webhooks
Enable allow/deny lists of remote hosts.

## Keepalives for activities, extending the lock until completion

## Future ideas
* Interactive CLI for execution management
* External activities gRPC API
* OpenAPI activity generator
* Spawning processes from WASM activities, reading their outputs
* Backpressure: Limits on pending queues, or an eviction strategy, slow down on `LimitReached`
* External executors support - starting executions solely based on WIT exports. External executors must share write access to the sqlite database.
* Labels restricting workflows/activities to executors
* Periodic scheduling
* [Deadline propagation](https://sre.google/sre-book/addressing-cascading-failures)
* [Cancellation propagation](https://sre.google/sre-book/addressing-cascading-failures)
* Queue capacity setting, adding backpressure to execution submission
* Ability to simulate behaviour of the system with injected failures
* Notify activities. When called, thir return value must be supplied via an API endpoint.
* An API for listing executions with their open notify activities.
* Read only query function that can be called during an await point or after execution finishes.
* Optional stdout,stderr persistence / forwarding
* Smart dependency routing from a caller via an interface import to one of many components that export it.
* Smart retries - Retry budget, disabling retries when the activity is failing certain % of requests
* Configurable jitter added to retries
* Workflow memory snapshots for faster replay
* Time traveling debugger for workflows, that works accross WASM deployments
* Ability to hotfix a set of workflows, with an approval system when non determinism is detected
* Trace strings to their origin accross workflows and activities
* Webhook endpoint mappings: running a single function, translating between HTTP and WIT defined parameters and return value
* Distributed tracing context forwarding for outgoing HTTP as well as webhooks
* Allow specifying permanent error variants in as annotations in WIT
* Support for (distributed) sagas - define rollbacks on activities, call them on failed workflows
