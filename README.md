# Obelisk
Deterministic workflow engine built on top of the WASM Component Model

## Project status / Disclaimer
This is a **pre-release**.

This repo contains backend code for local development and testing.
The software doesn't have backward compatibility guarantees for the CLI, gRPC or database schema.

### Supported platforms
* Linux x64

## Core principles
* Schema first, using [WASM Component model](https://component-model.bytecodealliance.org/)'s [WIT](https://component-model.bytecodealliance.org/design/wit.html) language as the interface between workflows and activities.
* Backend developer's delight
    * Single process for running the executor, workflows and activities, with an escape hatch for external activities (planned).
    * Automatic retries on errors, timeouts, workflow executions continuing after a server crash.
    * Observability (planned) - parameters and results together with function hierarchy must be preserved.
    * Composability - nesting workflows, calling activities written in any supported language
    * Replay and fork existing workflows(planned). Fix problems and continue.

## Concepts and features
* *Activities* that must be idempotent (retriable), so that they can be stopped and retried at any moment. This contract must be fulfilled by the activity itself.
    * WASI activities are executed in a WASM sandbox
        * Able to contact HTTP servers using the WASI 0.2 HTTP client.
        * Able to read/write to the filesystem (planned).
    * Max execution duration support, after which the execution is suspended into intermittent timeout.
    * Retries on errors - on WASM traps (panics), or when returning an Error result.
    * Retries on timeouts with exponential backoff.
    * Execution result is persisted.
    * Performance option to keep the parent workflow execution hot or unload and replay the event history.

* *Deterministic workflows*
    * Are replayable: Execution is persisted at every state change, so that it can be replayed after an interrupt or an error.
    * Running in a WASM sandbox, isolated from the environment
    * Automatically retried on failures like database errors, timeouts or even traps(panics).
    * Able to spawn child workflows or activities, either blocking until result arrives or awaiting the result asynchronously.
    * Workflows can be replayed with added log messages and other changes that do not alter the determinism of the execution (planned).
    * Join sets allow for structured concurrency, either blocking until child executions are done, or cancelling those that were not awaited (planned).

* *WASI webhooks*
    * Mounted as a URL path, serving HTTP traffic.
    * Able to spawn child workflows or activities.

* *Work stealing executor*
    * Periodically locking a batch of currently pending executions, starts/continues their execution
    * Cleaning up old hanging executions with expired locks. Executions that have the budget will be retried (planned).
    * Concurrency control - limit on the number of workers that can run simultaneously.

## Installation

### Docker
```sh
CONTAINER_ID=$(docker run -d getobelisk/obelisk)
docker logs --follow $CONTAINER_ID | grep "Serving gRPC requests"
```
```sh
docker exec $CONTAINER_ID obelisk client component list
# See Usage for more details
```

### Pre-built binary
Download [latest release](https://github.com/obeli-sk/obelisk/releases/latest) from the GitHub Release page.

### Using latest version from crates.io
Download using [cargo-binstall](https://crates.io/crates/cargo-binstall)
```sh
cargo binstall --locked obelisk
```
or build using [cargo](https://rustup.rs/)
```sh
cargo install --locked obelisk
```

### Nix flakes
```sh
nix --extra-experimental-features nix-command --extra-experimental-features flakes run github:obeli-sk/obelisk
```

## Usage
### Generating a sample configuration file
```sh
obelisk server generate-config
```

### Starting the server
```sh
obelisk server run
```

### Getting the list of loaded functions
```sh
obelisk client component list
```

### Submitting a function to execute (either workflow or activity)
```sh
# Call fibonacci(10) activity from the workflow 500 times in series.
obelisk client execution submit testing:fibo-workflow/workflow.fiboa '[10, 500]' --follow
```

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
- [ ] WIT Explorer
- [ ] Heterogenous join sets, allowing one join set to combine multiple function signatures and delays
- [ ] Expose filesystem with directory mapping for activities, webhooks
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
* Webhook mappings: running a single function, translating between HTTP and WIT defined parameters and return value
* Distributed tracing context forwarding for outgoing HTTP as well as webhooks
* Allow specifying permanent error variants in as annotations in WIT
* Support for (distributed) sagas - define rollbacks on activities, call them on failed workflows

# Building from source
Set up the development dependencies using nix flakes:
```sh
nix develop
# or `direnv allow`, after simlinking .envrc-example -> .envrc
```
Or manually download all dependencies, see [dev-deps.txt](dev-deps.txt) and [Ubuntu based verification Dockerfile](.github/workflows/release/verify/ubuntu-24.04-install.Dockerfile)
Run the program
```sh
cargo run --release
```

## Running Tests
```sh
./scripts/test.sh
```

## Deterministic tests using the `madsim` simulator
```sh
./scripts/test-madsim.sh
```

# Contributing
This project has a roadmap and features are added and tested in a certain order.
If you would like to contribute a feature, please [discuss](https://github.com/obeli-sk/obelisk/discussions) the feature on GitHub.
In order for us to accept patches and other contributions, you need to adopt our Contributor License Agreement (the "CLA"). The current version of the CLA can be found [here](https://cla-assistant.io/obeli-sk/obelisk).
