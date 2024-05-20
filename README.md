# Obelisk
Deterministic workflow engine.
Job scheduler/ WASM runner.

## Project status / Disclaimer
This is a pre-release.
This repo contains backend code for local development and testing.
The software doesn't have backward compatibility guarantees for CLI nor database format.
Please exercise caution if attempting to use it for production usage.

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
    * Calling a single child workflow or activity, blocking the execution
    * Execution is persisted at every state change, so that it can be replayed after an interrupt or an error.

* Work stealing executor
    * Periodically locking a batch of currently pending executions, starts/continues their execution
    * Cleaning up old hanging executions with expired locks. Executions that have the budget will be retried.

# Planned features
* UI
* OpenAPI binding generator
* Fatal error mapping to supported result types, e.g. permanent timeout to a numeric or string representation.
* WASM upgrades - disabling work stealing by executors with outdated wasm hashes
* Params typecheck on creation, introspection of types of all functions in the system
* Host function: generate a random value, store it in the event history - wasi random,available for workflows as wel
* Cancellation with recursion
* Limits on insertion of pending tasks or an eviction strategy like killing the oldest pending tasks.
* Labels restricting workflows/activities to executors
* ErrId that is passed back to parent, error detail

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

## Running

```sh
cargo run --features parallel-compilation
```

## Running Tests
```sh
cargo nextest run --workspace --target-dir=target/debug/nosim
```

### Deterministic tests using madsim simulator
```sh
MADSIM_ALLOW_SYSTEM_THREAD=1 RUSTFLAGS="--cfg madsim --cfg tracing_unstable" cargo nextest run --workspace --target-dir=target/debug/madsim
```
