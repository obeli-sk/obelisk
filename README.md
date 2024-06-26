# Obelisk
Deterministic workflow engine built on top of WASI Component Model

## Project status / Disclaimer
This is a pre-release.
This repo contains backend code for local development and testing.
The software doesn't have backward compatibility guarantees for CLI nor database format.
Please exercise caution if attempting to use it for production.

## Core principles
* Schema first, using [WIT](https://component-model.bytecodealliance.org/design/wit.html) as the interface specification between workflows and activities.
* Backend developer's delight
    * Single process for running the executor, wasm workflows and wasm activities, with an escape hatch for external activities (planned).
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
nix run github:obeli-sk/obelisk
```

## Local development
Set up the development dependencies using nix flakes:
```sh
nix develop
# or `direnv allow`, after simlinking .envrc-example -> .envrc
```
Or manually download
```sh
cargo install cargo-component --locked
```
Run the program
```sh
cargo run --release
```

## Usage

```sh
obelisk executor serve &
obelisk component add component.wasm # created by cargo component
obelisk component list
obelisk execution schedule <function> <params>
```

# Milestones

## Milestone 1: Release the binary - done
* Getting the `obelisk` application up and running as a Linux binary
* Scheduling of workflows and wasm activities, retries on timeouts and failures
* Persistence using sqlite
* Launching child workflows/activities concurrently using join sets
* Basic CLI for wasm component configuration and scheduling
* Github release, docker image, publish to crates.io, support `cargo-binstall`

## Milestone 2: Allow remote interaction via CLI and web UI
* Push/pull components to/from an OCI registry
* HTTP API for execution and component management
* Interactive CLI for execution and component management
* Params typecheck on creation, introspection of types of all functions in the system
* External process activities
* HTML based UI for showing executions, event history and relations

## Milestone 3
* URL paths with HTTP handlers registered by workflows, similar to the [proxy handler example](https://github.com/sunfishcode/hello-wasi-http/blob/main/src/lib.rs)
* External Activity RPC
* OpenAPI generator for activities
* Cancellation with recursion
* Limits on insertion of pending tasks or an eviction strategy like killing the oldest pending tasks.

## Milestone 4
* Multi process executors
* Labels restricting workflows/activities to executors
* ErrId that is passed back to parent, error detail

## Running Tests
```sh
cargo nextest run --workspace --target-dir=target/debug/nosim
```

### Deterministic tests using the `madsim` simulator
```sh
MADSIM_ALLOW_SYSTEM_THREAD=1 RUSTFLAGS="--cfg madsim" cargo nextest run --workspace --target-dir=target/debug/madsim
```
