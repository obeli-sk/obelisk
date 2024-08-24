# Obelisk
Deterministic workflow engine built on top of WASI Component Model

## Project status / Disclaimer
This is a **pre-release**.

This repo contains backend code for local development and testing.
The software doesn't have backward compatibility guarantees for CLI nor database format.
Please exercise caution if attempting to use it for production.

### Supported platforms
* Linux x64

## Core principles
* Schema first, using [WIT](https://component-model.bytecodealliance.org/design/wit.html) as the interface between workflows and activities.
* Backend developer's delight
    * Single process for running the executor, workflows and activities, with an escape hatch for external activities (planned).
    * Automatic retries on errors, timeouts, workflow executions continuing after a server crash.
    * Observability (planned) - parameters and results together with function hierarchy must be preserved.
    * Composability - nesting workflows, calling activities written in any supported language
    * Replay and fork existing workflows(planned). Fix problems and continue.
    * Time traveling debugger for workflows (planned)

## Concepts and features
* *Activities* that must be idempotent, so that they can be stopped and retried at any moment. This contract must be fulfilled by the activity itself.
    * WASI activities are executed in a WASM sandbox
        * Able to contact HTTP servers using the WASI 0.2 HTTP client.
        * Able to read/write to the filesystem (planned).
    * Max execution duration support, after which the execution is suspended into intermittent timeout.
    * Retries on errors - on WASM traps (panics), or when returning an Error result.
    * Retries on timeouts with exponential backoff.
    * Execution result is persisted.
    * Performance option to keep the parent workflow execution hot or unload and replay the event history.

* *Deterministic workflows*
    * Running in a WASM sandbox
    * Isolated from the environment
    * Able to spawn child workflows or activities, either blocking or awaiting the result eventually
    * Execution is persisted at every state change, so that it can be replayed after an interrupt or an error.
    * Ability to replay workflows with added log messages and other changes that do not alter the determinism of the execution (planned)

* *HTTP triggers* (planned)
    * Mounted as a URL path, serving HTTP traffic.
    * Able to spawn child workflows or activities.

* Work stealing executor
    * Periodically locking a batch of currently pending executions, starts/continues their execution
    * Cleaning up old hanging executions with expired locks. Executions that have the budget will be retried (planned).

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
Download [latest release](https://github.com/obeli-sk/obeli-sk/releases/latest) from the GitHub Release page.

### Using latest version from crates.io
Download using [cargo-binstall](https://crates.io/crates/cargo-binstall)
```sh
cargo binstall obeli-sk
```
or build
```sh
cargo install --locked obeli-sk
```

### Nix flakes
```sh
nix --extra-experimental-features nix-command --extra-experimental-features flakes run github:obeli-sk/obelisk
```

## Usage

### Starting the server
```sh
obelisk server run
```

### Getting the list of loaded functions
```sh
obelisk client component list

wasm_workflow   fibo_workflow   wasm_workflow:sha256:885d60e8d9b09fadecae99d6143ec65ad819e1991275cba78e2431619381da75
Exports:
        testing:fibo-workflow/workflow.fiboa : func(n: u8, iterations: u32) -> u64
...
```

### Executing a sample workflow
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

## Milestone 2: Allow remote interaction via CLI and web UI - started
- [x] Move component and general configuration into a TOML file
- [x] Pull components -from an OCI registry
- [x] Publish the obelisk image to the Docker Hub (ubuntu, alpine)
- [x] obelisk client component push
- [x] gRPC API for execution management
- [x] Track the topmost parent
- [ ] Interactive CLI for execution management
- [x] Params typecheck on creation, introspection of types of all functions in the system
- [ ] HTML based UI for showing executions, event history and relations
- [x] Logging and tracing configuration, sending events to an OTLP collector

## Milestone 3
- [ ] HTTP webhook triggers, similar to the [proxy handler example](https://github.com/sunfishcode/hello-wasi-http/blob/main/src/lib.rs)
- [ ] External activities
- [ ] Add examples with C#, Go, JS, Python

## Milestone 4
- [ ] OpenAPI activity generator
- [ ] Limits on insertion of pending tasks or an eviction strategy like killing the oldest pending tasks.
- [ ] Multi process executors
- [ ] Labels restricting workflows/activities to executors
- [ ] Periodic scheduling
- [ ] [Deadline propagation](https://sre.google/sre-book/addressing-cascading-failures)
- [ ] [Cancellation propagation](https://sre.google/sre-book/addressing-cascading-failures)
- [ ] Queue capacity setting, adding backpressure to execution submission
- [ ] Retry budget, disabling retries when the activity is failing certain % of requests

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
If you would like to contribute a feature, please discuss the feature in an issue on this GitHub repository.
In order for us to accept patches and other contributions, you need to adopt our Contributor License Agreement (the "CLA"). The current version of the CLA can be found [here](https://cla-assistant.io/obeli-sk/obelisk).
