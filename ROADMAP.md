# Immediate goals

## feat: Expose network configuration for activities, webhooks
Enable allow/deny lists of remote hosts.

## Upcoming goals

## feat: Add `obelisk generate`
`obelisk generate config` blank(just webui),fibo, testing, stargazers
`obelisk generate wit -c obelisk.toml --out-dir wit/deps/ my-activity`
`obelisk generate wit` - based on obelisk-deps.toml tool + extensions based on component type, e.g. just `-schedule` for webhooks
`obelisk new` - show templates, blank workflow should have obelisk types and workflow support
`obelisk add --oci path` - add WIT files + extensions

## feat: Inspect remote or cached components
`obelisk client component inspect/wit` should accept: path, componentId, oci location

## feat: obelisk.lock
Save the digest of each downloaded OCI image into a lockfile, then remove the optional digest from toml,

## feat: Support WIT-only WASM files
Allow pushing and declaring WASM resources only containing the WIT.
Needed for external activities.

## feat: Long running monitor trigger
Similar to a webhook endpoint, new component type with `main`, restarts on exit or trap.
Can listen to a HTTP stream and trigger an execution.
Could be used to monitor MQTT, UDP etc.

## feat: Keepalives for activities, extending the lock until completion

## fix: Change FFQN - allow dots in function name
Accomodate for resource functions.

## fix: Migrate logging to wasi
Use wasi:logging/logging@0.1.0-draft

## Future ideas
* Optional caching of activity executions with a TTL - serve cached response if parameters are the same
* External activities gRPC API
* External executors support - starting executions solely based on WIT exports. External executors must share write access to the sqlite database.
* Backpressure: Limits on pending queues, or an eviction strategy, slow down on `LimitReached`
* Labels restricting workflows/activities to executors
* Periodic scheduling
* [Deadline propagation](https://sre.google/sre-book/addressing-cascading-failures)
* [Cancellation propagation](https://sre.google/sre-book/addressing-cascading-failures)
* Queue capacity setting, adding backpressure to execution submission
* Optional stdout,stderr persistence / forwarding
* Smart dependency routing from a caller via an interface import to one of many components that export it.
* Smart retries - Retry budget, disabling retries when the activity is failing certain % of requests
* Configurable jitter added to retries
* Workflow memory snapshots for faster replay
* Ability to hotfix a set of workflows, with an approval system when non determinism is detected - forking the execution log
* Webhook endpoint mappings: running a single function, translating between HTTP and WIT defined parameters and return value
* Distributed tracing context forwarding for outgoing HTTP as well as webhooks
* Support for (distributed) sagas - define rollbacks on activities, call them on failed workflows
* Investigate code-coverage for workflow steps
