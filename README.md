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

* Multi-master scheduler
    * Periodically locks a batch of currently pending executions, starts/continues their execution
    * Cleans up old hanging executions with expired locks. Executions that have the budget will be retried.

# Planned features
* Host function: generate a random value
* Allow starting multiple child executions
* Persistence using sqlite
* Persistence using postgresql
* Concurrent non blocking child executions
* Cancellation
* Limits on insertion of pending tasks or an eviction strategy like killing the oldest pending tasks.

## Setting up environment
```sh
nix develop # or direnv allow, after simlinking .envrc-example -> .envrc
```

## Building
```sh
cargo build --workspace --no-default-features
```

## Tests
```sh
RUSTFLAGS="--cfg tokio_unstable --cfg tracing_unstable" cargo test --workspace
```

### Deterministic tests using madsim simulator
```sh
RUSTFLAGS="--cfg madsim --cfg tokio_unstable --cfg tracing_unstable" cargo test --workspace
```

