# Milestones

## Milestone 1: Release the binary - done
- [x] Getting the `obelisk` application up and running as a Linux binary
- [x] Scheduling of workflows and wasm activities, retries on timeouts and failures
- [x] Persistence using sqlite
- [x] Launching child workflows/activities concurrently using join sets
- [x] Basic CLI for wasm component configuration and scheduling
- [x] Github release, docker image, publish to crates.io, support `cargo-binstall`

## Milestone 2: Allow remote interaction via CLI - done
- [x] Move component and general configuration into a TOML file
- [x] Pull components -from an OCI registry
- [x] Publish the obelisk image to the Docker Hub (ubuntu, alpine)
- [x] obelisk client component push
- [x] gRPC API for execution management
- [x] Track the topmost parent
- [x] Params typecheck on creation, introspection of types of all functions in the system
- [x] Logging and tracing configuration, sending events to an OTLP collector

## Milestone 3: Webhooks, Verify, Structured concurrency, Web UI - started
- [x] HTTP webhook triggers able to start new executions (workflows and activities), able to wait for result before sending the response.
- [x] Forward stdout and stderr (configurable) of activities and webhooks
- [x] Support for distributed tracing, logging from components collected by OTLP
- [x] Mapping from any execution result (e.g. traps, timeouts, err variants) to other execution results via `-await-next`
- [x] Server verification - downloads components, checks the TOML configuration and matches component imports with exports.
- [x] Structured concurrency for join sets - blocking parent until all child executions are finished
- [x] HTML based UI for showing executions, event history and relations
- [x] WIT Explorer
- [ ] Heterogenous join sets, allowing one join set to combine multiple function signatures and delays
- [ ] Expose network configuration for activities, webhooks
- [ ] Keepalives for activities, extending the lock until completion
- [ ] Examples with C#, Go, JS, Python

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
