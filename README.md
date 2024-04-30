# obeli-sk
Deterministic workflow engine.
Job scheduler/ WASM runner.

## Project status
This is a pre-release.

## Features
* Idempotent *activities*
    * Able to contact HTTP servers using WASI 0.2 HTTP client.
    * Max execution duration support, after which the execution is suspended into intermittent timeout
    * Retries on errors (both wasm traps (panics), or returning an Error result)
    * Retries on timeouts
    * Exponential backoff
    * Execution result is persisted
    * Option to keep the parent workflow execution hot or unload and replay the event history.

* Deterministic *workflows*
    * Calling a single child workflow or activity, blocking the execution
    * Execution is persisted at every state change, so that it can be replayed after an interrupt or an error.

* Work stealing executor
    * Periodically locking a batch of currently pending executions, starts/continues their execution
    * Cleaning up old hanging executions with expired locks. Executions that have the budget will be retried.

# Planned features
* Configurable `Error` retries, disabled for workflows
* Allow starting multiple child executions in parallel
* TUI
* Structured concurrency patterns (join, race)
* WASM upgrades - disabling work stealing by executors with outdated wasm hashes
* Params typecheck on creation, introspection of types of all functions in the system
* Host function: generate a random value, store it in the event history
* Persistence using postgresql
* Cancellation
* Limits on insertion of pending tasks or an eviction strategy like killing the oldest pending tasks.
* Labels restricting workflows/activities to executors

# Building

## Setting up environment
Using nix flakes:
```sh
nix develop # or direnv allow, after simlinking .envrc-example -> .envrc
```
Otherwise:
```sh
cargo install cargo-component
```

## Building

```sh
cargo build --workspace
```

## Running Tests
```sh
cargo test --workspace
```

### Deterministic tests using madsim simulator
```sh
RUSTFLAGS="--cfg madsim --cfg tracing_unstable" cargo test --workspace
```
