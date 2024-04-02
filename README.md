## Features
* Executing idempotent activities, which may use WASI 0.2 HTTP client.
    * Max execution duration support, after which the execution is suspended into intermittent timeout
    * Retries on errors (both wasm traps (panics), or returning an Error result)
    * Retries on timeouts
    * Exponential backoff
    * Execution result is persisted

* Executing deterministic workflows
    * Calling a single workflow / activity, blocking the execution
    * Execution is persisted at every state change, so that it can be replayed after an interrupt

* Work stealing executor
    * Periodically locks a batch of currently pending executions, starts/continues their execution
    * Cleans up old hanging executions with expired locks. Executions that have the budget will be retried.

# Planned features
* Host function: generate a random value
* Allow starting multiple child executions in parallel
* Allow scheduling new execution from the current one
* Persistence using sqlite
* CLI + server binary
* Persistence using postgresql
* Advanced concurrency patterns
* Cancellation
* Limits on insertion of pending tasks or an eviction strategy like killing the oldest pending tasks.
* Labels restricting workflows/activities to executors

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

## Tests
```sh
RUSTFLAGS="--cfg tracing_unstable" cargo test --workspace
```

### Deterministic tests using madsim simulator
```sh
RUSTFLAGS="--cfg madsim --cfg tracing_unstable" cargo test --workspace
```

