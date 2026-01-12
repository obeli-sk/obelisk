# Immediate goals

# Upcoming goals

## Add WebAPI and DB JSON Schemas

## feat: `generate wit-extensions` with specified interface(s)
Allows self referential exports, also enables generating on import side.

## feat: Add `obelisk generate project`
show templates, blank workflow should have obelisk types and workflow support

## feat: Multiple versions of a component
dont use global registry, mock locally using imports, allow same component twice. Toml dependency routing using `requires = "auto" | ["dep1"]`

## feat: KV host activity
Allow using the underlying database.
Also a conf KV backed by toml file to configure workflows.

## feat: Retention
Perform a cascading delete of top-level executions that finished more than a certain number of days ago.

## feat: Multiple execution queues
Allow specifying queue ID when submitting, and when configuring an executor.

## feat: Configurable network access for activities, webhooks
Enable allow/deny lists of remote hosts for HTTP client.

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
* Allow setting retry config during execution creation using an extension function
* Configurable jitter added to retries
* Workflow memory snapshots for faster replay
* Ability to hotfix a set of workflows, with an approval system when non determinism is detected - forking the execution log
* Webhook endpoint mappings: running a single function, translating between HTTP and WIT defined parameters and return value
* Distributed tracing context forwarding for outgoing HTTP as well as webhooks
* Investigate code-coverage for workflow steps
