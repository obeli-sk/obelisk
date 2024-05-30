# Obelisk
Deterministic workflow engine built on top of WASI 0.2

## Project status / Disclaimer
This is a pre-release.
This repo contains backend code for local development and testing.
The software doesn't have backward compatibility guarantees for CLI nor database format.
Please exercise caution if attempting to use it for production.

## Core principles
* Schema first, using [WIT](https://component-model.bytecodealliance.org/design/wit.html) as the interface specification between workflows and activities.
* Local first development, single process, with an escape hatch for external activities (planned).
* Backend developer's delight
    * Automatic retries on errors, timeouts, workflow executions continuing after a server crash.
    * Observability (planned)
    * Time traveling debugger (planned), ability to replay and fork existing workflow executions.

## Features
* *Activities* that must be idempotent
    * Able to contact HTTP servers using WASI 0.2 HTTP client.
    * Max execution duration support, after which the execution is suspended into intermittent timeout
    * Retries on errors (both wasm traps (panics), or returning an Error result)
    * Retries on timeouts
    * Exponential backoff
    * Execution result is persisted
    * Performance option to keep the parent workflow execution hot or unload and replay the event history.

* *Deterministic workflows*
    * Isolated from the environment
    * Workflows can spawn child workflows or activities, either blocking or awaiting the result eventually
    * Execution is persisted at every state change, so that it can be replayed after an interrupt or an error.
    * Ability to replay workflows with added log messages and other changes that do not alter the determinism of the execution (planned)

* Work stealing executor
    * Periodically locking a batch of currently pending executions, starts/continues their execution
    * Cleaning up old hanging executions with expired locks. Executions that have the budget will be retried.

## Installation

### Docker
TODO

### Pre-built binary
TODO

### Building from crates.io
```sh
# Install rust from https://rustup.rs/
cargo install --locked obeli-sk
```

## Building from source
Set up the environment using nix flakes:
```sh
nix develop # or direnv allow, after simlinking .envrc-example -> .envrc
```
Or manually:
```sh
cargo install cargo-component --locked
```
Run the program:
```sh
cargo run --release
```

## Usage

```sh
obeli-sk server run &
obeli-sk worker load github:obeli-sk/examples@latest
obeli-sk function list
obeli-sk function run ...
```

# Planned features
* UI
* External Activity RPC
* OpenAPI generator for activities
* Fatal error mapping to supported result types, e.g. permanent timeout to a numeric or string representation.
* WASM upgrades - disabling work stealing by executors with outdated wasm hashes
* Params typecheck on creation, introspection of types of all functions in the system
* Host function: generate a random value, store it in the event history - wasi random,available for workflows as wel
* Cancellation with recursion
* Limits on insertion of pending tasks or an eviction strategy like killing the oldest pending tasks.
* Labels restricting workflows/activities to executors
* ErrId that is passed back to parent, error detail
* Ability to run activities in any language using an RPC protocol


## Running

```sh
cargo run
```

## Running Tests
```sh
cargo nextest run --workspace --target-dir=target/debug/nosim
```

### Deterministic tests using madsim simulator
```sh
MADSIM_ALLOW_SYSTEM_THREAD=1 RUSTFLAGS="--cfg madsim" cargo nextest run --workspace --target-dir=target/debug/madsim
```
